"""调用限流降级：阈值校验、判定口径、回执幂等与恢复窗口的回归用例。"""
from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services import limiter as limiter_module
from app.services.limiter import LimiterService
from app.store import store

client = TestClient(app)
service = LimiterService()


@pytest.fixture(autouse=True)
def fresh_store() -> None:
    """每个用例都从种子数据重新开始，避免判定状态互相污染。"""
    from app.seed import SEED_ROWS

    with limiter_module._LOCK:
        store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    yield


def rule_row(code: str = "LIMI-0002") -> dict:
    for row in store.rows("limiter"):
        if row.get("规则编号") == code:
            return row
    raise AssertionError(code)


def test_rule_catalog_exposes_threshold_fields() -> None:
    payload = client.get("/api/limiter/rules").json()
    assert "限流阈值" in payload["fields"]
    assert "判定口径" in payload["fields"]
    assert payload["bounds"]["每秒上限"] == {"min": 1, "max": 1_000_000}
    assert payload["bounds"]["恢复时间"]["unit"] == "秒"


def test_three_requests_first_two_pass_third_degrades() -> None:
    """LIMI-0002：每秒上限 2 / 突发容量 3 / 恢复时间 60，前两次放行、第三次降级。"""
    now = "2026-11-14T10:00:00+00:00"
    decisions = [service.check("LIMI-0002", at=now)[0]["判定结果"] for _ in range(3)]
    assert decisions == ["放行", "放行", "降级"]

    receipts = service.list_receipts("LIMI-0002")
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt["触发阈值"] == "每秒上限"
    assert receipt["触发阈值值"] == 2
    assert receipt["窗口内已通过"] == 2
    assert receipt["状态"] == "生效中"
    assert receipt["触发阈值明细"]["限流阈值"] == {"阈值": 2, "实际": 2}
    assert receipt["触发阈值明细"]["每秒上限"]["阈值"] == 2


def test_repeated_trigger_does_not_append_second_receipt() -> None:
    now = "2026-11-14T10:00:00+00:00"
    for _ in range(3):
        service.check("LIMI-0002", at=now)
    first = service.check("LIMI-0002", at=now)[2]
    second = service.check("LIMI-0002", at=now)[2]
    assert first["回执编号"] == second["回执编号"]
    assert len(service.list_receipts("LIMI-0002")) == 1


def test_receipt_keeps_threshold_snapshot_after_rule_change() -> None:
    now = "2026-11-14T10:00:00+00:00"
    for _ in range(3):
        service.check("LIMI-0002", at=now)
    before = dict(service.list_receipts("LIMI-0002")[0])

    entry, errors = service.update_entry(2, {"限流阈值": 1, "每秒上限": 1, "恢复时间": 120})
    assert errors == []
    assert entry["限流阈值"] == 1

    after = service.list_receipts("LIMI-0002")[0]
    for field in ("限流阈值", "每秒上限", "突发容量", "判定口径", "触发阈值值"):
        assert after[field] == before[field], field
    checks = service.list_checks("LIMI-0002")
    assert checks[-1]["每秒上限"] == 2


def test_recovery_starts_a_new_window_and_recounts() -> None:
    start = "2026-11-14T10:00:00+00:00"
    for _ in range(3):
        service.check("LIMI-0002", at=start)

    early, early_message, _ = service.restore("LIMI-0002", at="2026-11-14T10:00:59+00:00")
    assert early is None
    assert "恢复时间未到" in early_message

    check, message, _ = service.restore("LIMI-0002", at="2026-11-14T10:01:00+00:00")
    assert check["判定结果"] == "恢复"
    assert check["窗口序号"] == 2
    assert "重新计数" in message
    assert rule_row()["status"] == "生效中"
    assert service.list_receipts("LIMI-0002")[0]["状态"] == "已恢复"

    decisions = [
        service.check("LIMI-0002", at="2026-11-14T10:01:01+00:00")[0]["判定结果"]
        for _ in range(3)
    ]
    assert decisions == ["放行", "放行", "降级"]
    assert rule_row()["当前窗口"] == 2
    assert len(service.list_receipts("LIMI-0002")) == 2


def test_window_rolls_over_automatically_without_explicit_restore() -> None:
    start = "2026-11-14T10:00:00+00:00"
    for _ in range(3):
        service.check("LIMI-0002", at=start)
    check, _, _ = service.check("LIMI-0002", at="2026-11-14T10:02:00+00:00")
    assert check["窗口序号"] == 2
    assert check["判定结果"] == "放行"
    assert rule_row()["status"] == "生效中"


def test_route_service_switches_to_degraded_and_back() -> None:
    now = "2026-11-14T10:00:00+00:00"
    route = store.find("router", 2)
    assert route["当前路由"] == "订单查询服务"

    for _ in range(3):
        service.check("LIMI-0002", at=now)
    assert route["当前路由"] == "订单查询降级副本"
    assert route["路由状态"] == "降级"
    assert route["status"] == "已降级"
    assert service.list_receipts("LIMI-0002")[0]["路由同步"] == "已同步"

    service.restore("LIMI-0002", at="2026-11-14T10:01:00+00:00")
    assert route["当前路由"] == "订单查询服务"
    assert route["路由状态"] == "正常"
    assert route["status"] == "生效中"


def test_concurrent_requests_for_last_burst_slot_only_one_passes() -> None:
    """突发容量剩下 1 个额度时，两个并发判定只能有一个进入。"""
    # 每条规则先放行到只剩一个突发额度；阈值设在突发容量上，避免提前触发。
    entry, errors = service.update_entry(2, {"限流阈值": 3, "每秒上限": 3, "突发容量": 3})
    assert errors == []
    now = "2026-11-14T10:00:00+00:00"
    for _ in range(2):
        assert service.check("LIMI-0002", at=now)[0]["判定结果"] == "放行"

    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def worker() -> None:
        barrier.wait()
        check = service.check("LIMI-0002", at=now)[0]
        outcomes.append(check["判定结果"])

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(outcomes) == ["放行", "降级"]
    assert len(service.list_receipts("LIMI-0002")) == 1
    assert entry["当前窗口"] == 1


def test_threshold_out_of_range_is_rejected() -> None:
    cases = [
        ({"限流阈值": 4, "每秒上限": 3, "突发容量": 3}, "不能超过突发容量"),
        ({"每秒上限": 5, "突发容量": 3}, "不能小于每秒上限"),
        ({"限流阈值": 1, "每秒上限": 3, "突发容量": 3}, "不能小于每秒上限"),
        ({"每秒上限": 0}, "每秒上限需在 1~1000000 之间"),
        ({"突发容量": -1}, "突发容量需在 1~1000000 之间"),
        ({"恢复时间": 0}, "恢复时间须为"),
        ({"判定口径": "随机放行"}, "判定口径须为"),
        ({"限流阈值": "abc"}, "限流阈值必须是整数"),
    ]
    for override, expected in cases:
        values = {
            "规则编号": "LIMI-9001",
            "限流对象": "订单查询服务",
            "限流算法": "令牌桶",
            "限流阈值": 2,
            "每秒上限": 2,
            "突发容量": 3,
            "判定口径": "令牌桶",
            "降级动作": "切换降级副本",
            "恢复时间": 60,
        }
        values.update(override)
        entry, errors = service.create_entry(values)
        assert entry is None
        assert any(expected in error for error in errors), (override, errors)


def test_boundary_threshold_is_accepted() -> None:
    entry, errors = service.create_entry({
        "规则编号": "LIMI-9002",
        "限流对象": "边界服务",
        "限流算法": "滑动窗口",
        "限流阈值": 5,
        "每秒上限": 5,
        "突发容量": 5,
        "判定口径": "滑动窗口",
        "降级动作": "切换降级副本",
        "恢复时间": 1,
    })
    assert errors == []
    assert entry["限流阈值"] == 5
    assert entry["status"] == "草稿"


def test_http_api_full_cycle() -> None:
    base = {"规则编号": "LIMI-9003"}
    values = {
        **base,
        "限流对象": "订单查询服务",
        "限流算法": "令牌桶",
        "限流阈值": 2,
        "每秒上限": 2,
        "突发容量": 3,
        "判定口径": "令牌桶",
        "降级动作": "切换降级副本",
        "恢复时间": 60,
        "关联路由": "ROUT-0002",
    }
    created = client.post("/api/limiter", json={"values": values}).json()
    assert created["ok"] is True
    entry_id = created["entry"]["id"]

    invalid = client.put(f"/api/limiter/{entry_id}", json={"values": {"限流阈值": 99}}).json()
    assert invalid["ok"] is False
    assert "不能超过突发容量" in invalid["message"]

    enabled = client.post(f"/api/limiter/{entry_id}/actions", json={"values": {"action": "启用限流"}}).json()
    assert enabled["ok"] is True

    at = "2026-11-14T10:00:00+00:00"
    results = [
        client.post(f"/api/limiter/{entry_id}/checks", json={"values": {"at": at}}).json()
        for _ in range(3)
    ]
    assert [item["entry"]["判定结果"] for item in results] == ["放行", "放行", "降级"]
    assert results[2]["receipt"]["窗口序号"] == 1

    duplicate = client.post(f"/api/limiter/{entry_id}/checks", json={"values": {"at": at}}).json()
    assert duplicate["receipt"]["回执编号"] == results[2]["receipt"]["回执编号"]
    receipts = client.get("/api/limiter/receipts", params={"rule_code": "LIMI-9003"}).json()
    assert receipts["total"] == 1

    early = client.post(f"/api/limiter/{entry_id}/restore", json={"values": {"at": "2026-11-14T10:00:30+00:00"}}).json()
    assert early["ok"] is False
    restored = client.post(f"/api/limiter/{entry_id}/restore", json={"values": {"at": "2026-11-14T10:01:00+00:00"}}).json()
    assert restored["ok"] is True

    route = client.get("/api/router/2").json()
    assert route["当前路由"] == "订单查询服务"
    assert route["路由状态"] == "正常"
