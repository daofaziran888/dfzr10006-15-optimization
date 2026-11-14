"""调用限流降级业务规则：阈值校验、窗口判定、降级回执与路由同步。

判定模型
--------
一条限流规则由四组参数共同决定是否触发：

* ``判定口径``：令牌桶、固定窗口或滑动窗口，决定每秒额度与突发额度如何核算；
* ``每秒上限``：令牌补充速率（次/秒），固定与滑动窗口下即最近 1 秒的放行上限；
* ``突发容量``：令牌桶容量，新窗口装满，同一限流对象可在容量内短时通过；
* ``限流阈值``：当前恢复窗口内允许放行的累计请求数，达到即降级，须不小于每秒上限
  且不超过突发容量。

判定顺序：已降级窗口直接复用回执；否则先核算每秒额度，再检查累计限流阈值，任一越界
都会生成本窗口唯一的一份降级回执，回执里写明触发的阈值名称、数值与判定口径。降级状态
保持到恢复时间到达，恢复后从新窗口重新计数。回执与判定流水都保存触发当时的阈值快照，
之后修改规则不会改写历史判定。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.router import RouterService
from app.store import store

MODULE = "limiter"
RECEIPT_MODULE = "_limiter_receipts"
CHECK_MODULE = "_limiter_checks"
WINDOW_MODULE = "_limiter_windows"

# FastAPI 的同步路由跑在线程池里；同一进程内的判定必须串行化，
# 两个请求争抢最后一个突发额度时只能有一个进入。
_LOCK = threading.RLock()
_ROUTER_SERVICE = RouterService()

REQUIRED_FIELDS = [
    "规则编号",
    "限流对象",
    "限流算法",
    "每秒上限",
    "突发容量",
    "降级动作",
    "恢复时间",
]
OPTIONAL_FIELDS = ["关联路由", "限流阈值", "判定口径"]

STATUS_ORDER = ["草稿", "生效中", "已触发", "已关闭"]
ACTION_TARGETS = {"启用限流": "生效中", "触发降级": "已触发", "关闭限流": "已关闭"}

BASIS_ALIASES = {
    "令牌桶": "令牌桶",
    "token bucket": "令牌桶",
    "token_bucket": "令牌桶",
    "漏桶": "令牌桶",
    "leaky bucket": "令牌桶",
    "固定窗口": "固定窗口",
    "fixed window": "固定窗口",
    "fixed_window": "固定窗口",
    "滑动窗口": "滑动窗口",
    "sliding window": "滑动窗口",
    "sliding_window": "滑动窗口",
}
BASIS_ORDER = ["令牌桶", "固定窗口", "滑动窗口"]

# 数值边界：超出边界的配置不允许保存。
LIMIT_MIN, LIMIT_MAX = 1, 1_000_000
BURST_MIN, BURST_MAX = 1, 1_000_000
THRESHOLD_MIN, THRESHOLD_MAX = 1, 1_000_000
RECOVERY_MIN, RECOVERY_MAX = 1, 86_400


class LimiterValidationError(ValueError):
    """配置校验失败：把逐条原因一起返回，方便接口原样展示。"""

    def __init__(self, errors: list[str]) -> None:
        super().__init__("；".join(errors))
        self.errors = errors


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_int(value: Any, field: str, minimum: int, maximum: int) -> int:
    raw = _text(value)
    if not raw:
        raise ValueError(f"{field}不能为空")
    try:
        number = float(raw)
    except ValueError as exc:
        raise ValueError(f"{field}必须是整数，当前为「{raw}」") from exc
    if not number.is_integer():
        raise ValueError(f"{field}必须是整数，当前为「{raw}」")
    result = int(number)
    if result < minimum or result > maximum:
        raise ValueError(f"{field}需在 {minimum}~{maximum} 之间，当前为 {result}")
    return result


def _parse_recovery_seconds(value: Any) -> int:
    raw = _text(value)
    if raw.endswith("秒"):
        raw = raw[:-1].strip()
    try:
        return _parse_int(raw, "恢复时间", RECOVERY_MIN, RECOVERY_MAX)
    except ValueError as exc:
        raise ValueError(f"恢复时间须为 {RECOVERY_MIN}~{RECOVERY_MAX} 秒的整数（当前为「{_text(value)}」）") from exc


def _parse_basis(value: Any) -> str:
    raw = _text(value)
    key = raw.lower()
    if key not in BASIS_ALIASES:
        raise ValueError(f"判定口径须为 {'、'.join(BASIS_ORDER)} 之一，当前为「{raw}」")
    return BASIS_ALIASES[key]


def _parse_at(value: Any) -> datetime | None:
    """把请求里的模拟时间解析成 UTC 时间；未传时返回 None（由调用方取当前时间）。"""
    if value is None or _text(value) == "":
        return None
    if isinstance(value, datetime):
        moment = value
    else:
        raw = _text(value).replace("Z", "+00:00")
        try:
            moment = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"调用时间须为 ISO 8601 时间，当前为「{_text(value)}」") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _iso(moment: datetime | None) -> str | None:
    return None if moment is None else moment.astimezone(timezone.utc).isoformat()


def _as_datetime(value: Any) -> datetime:
    parsed = _parse_at(value)
    if parsed is None:
        raise ValueError("时间字段不能为空")
    return parsed


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _latest_window(rule_code: str) -> dict[str, Any] | None:
    for row in reversed(store.rows(WINDOW_MODULE)):
        if row.get("规则编号") == rule_code:
            return row
    return None


def _snapshot(values: dict[str, Any]) -> dict[str, Any]:
    """判定或触发时冻结的阈值口径，历史记录只读这份快照。"""
    return {
        "限流阈值": values.get("限流阈值"),
        "每秒上限": values.get("每秒上限"),
        "突发容量": values.get("突发容量"),
        "判定口径": values.get("判定口径"),
        "恢复时间": values.get("恢复时间"),
    }


def _find_route_code(entry: dict[str, Any]) -> str:
    route_code = _text(entry.get("关联路由"))
    if route_code:
        return route_code
    # 未显式关联时，按「目标服务 = 限流对象」兜底匹配一条路由规则。
    for route in store.rows("router"):
        if _text(route.get("目标服务")) == _text(entry.get("限流对象")):
            return _text(route.get("规则编号"))
    return ""


class LimiterService:
    # ------------------------------------------------------------------ 列表与配置
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("规则编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def _normalize_rule(self, values: dict[str, Any]) -> dict[str, Any]:
        missing = [field for field in REQUIRED_FIELDS if not _text(values.get(field))]
        if missing:
            raise LimiterValidationError([f"缺少必填字段：{field}" for field in missing])

        errors: list[str] = []

        def parse(field: str, parser: Any) -> Any:
            try:
                return parser()
            except ValueError as exc:
                errors.append(str(exc))
                return None

        limit = parse("每秒上限", lambda: _parse_int(values.get("每秒上限"), "每秒上限", LIMIT_MIN, LIMIT_MAX))
        burst = parse("突发容量", lambda: _parse_int(values.get("突发容量"), "突发容量", BURST_MIN, BURST_MAX))
        # 限流阈值与判定口径可以省略：阈值默认等于每秒上限，口径默认令牌桶。
        threshold = parse(
            "限流阈值",
            lambda: _parse_int(
                values.get("限流阈值") if _text(values.get("限流阈值")) else (limit if limit is not None else ""),
                "限流阈值", THRESHOLD_MIN, THRESHOLD_MAX,
            ),
        )
        basis = parse(
            "判定口径",
            lambda: _parse_basis(values.get("判定口径") if _text(values.get("判定口径")) else "令牌桶"),
        )
        recovery = parse("恢复时间", lambda: _parse_recovery_seconds(values.get("恢复时间")))

        if threshold is not None and limit is not None and threshold < limit:
            errors.append(f"限流阈值（{threshold}）不能小于每秒上限（{limit}）")
        if burst is not None and limit is not None and burst < limit:
            errors.append(f"突发容量（{burst}）不能小于每秒上限（{limit}）")
        if threshold is not None and burst is not None and threshold > burst:
            errors.append(f"限流阈值（{threshold}）不能超过突发容量（{burst}）")
        if errors:
            raise LimiterValidationError(errors)

        normalized: dict[str, Any] = {
            "限流阈值": threshold,
            "每秒上限": limit,
            "突发容量": burst,
            "判定口径": basis,
            "恢复时间": recovery,
        }
        for field in ("规则编号", "限流对象", "限流算法", "降级动作"):
            normalized[field] = _text(values.get(field))
        if "关联路由" in values:
            normalized["关联路由"] = _text(values.get("关联路由"))
        return normalized

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        try:
            normalized = self._normalize_rule(values)
        except LimiterValidationError as exc:
            return None, exc.errors
        rule_code = normalized["规则编号"]
        if any(row.get("规则编号") == rule_code for row in store.rows(MODULE)):
            return None, [f"规则编号 {rule_code} 已存在，不能重复登记"]
        rows = store.rows(MODULE)
        entry = {"id": _next_id(rows)}
        entry.update(normalized)
        entry.update({
            "status": "草稿",
            "规则状态": "草稿",
            "pending": True,
            "abnormal": False,
            "窗口状态": "未启用",
            "当前窗口": 0,
            "最近判定": "",
            "最近判定原因": "",
            "最近判定时间": None,
            "最近回执": None,
        })
        rows.append(entry)
        return entry, []

    def update_entry(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, [f"限流规则 {entry_id} 不存在或已归档"]
        merged = {field: entry.get(field) for field in REQUIRED_FIELDS + OPTIONAL_FIELDS}
        merged.update({field: values.get(field) for field in REQUIRED_FIELDS + OPTIONAL_FIELDS if field in values})
        try:
            normalized = self._normalize_rule(merged)
        except LimiterValidationError as exc:
            return None, exc.errors
        entry.update(normalized)
        entry["pending"] = entry.get("status") != STATUS_ORDER[-1]
        return entry, []

    # ------------------------------------------------------------------ 状态动作
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"限流规则 {entry_id} 不存在或已归档"
        if action not in ACTION_TARGETS:
            return None, f"动作「{action}」不属于调用限流降级可执行范围"
        target = ACTION_TARGETS[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        with _LOCK:
            now = datetime.now(timezone.utc)
            if target == "已触发":
                # 人工触发同样按当前窗口生成回执：同一窗口重复触发复用第一份。
                window = self._window_for(entry, now)
                limit = int(window.get("每秒上限") or LIMIT_MIN)
                threshold = int(window.get("限流阈值") or THRESHOLD_MIN)
                passed = int(window.get("已通过", 0))
                # 与自动判定保持同一优先级：累计达到限流阈值优先于每秒额度。
                if passed >= threshold:
                    trigger, trigger_value = "限流阈值", threshold
                else:
                    trigger, trigger_value = "每秒上限", limit
                reason = f"人工触发降级，按{trigger} {trigger_value} 记录回执"
                window["状态"] = "降级中"
                receipt = self._open_receipt(
                    entry, window, now,
                    trigger=trigger, trigger_value=trigger_value, reason=reason,
                )
                self._sync_route(entry, receipt, degrade=True, now=now)
                entry["窗口状态"] = "降级中"
                entry["当前窗口"] = int(window.get("窗口序号", 1))
                entry["最近判定"] = "降级"
                entry["最近判定原因"] = reason
                entry["最近判定时间"] = _iso(now)
                entry["最近回执"] = receipt.get("回执编号")
            elif target == "生效中":
                self._sync_route(entry, None, degrade=False, now=now)
                if _text(entry.get("窗口状态")) in ("", "未启用", "已关闭", "已恢复"):
                    entry["窗口状态"] = "计数中"
            elif target == "已关闭":
                self._sync_route(entry, None, degrade=False, now=now)
                entry["窗口状态"] = "已关闭"

            entry["status"] = target
            entry["规则状态"] = target
            entry["pending"] = target != STATUS_ORDER[-1]
            entry["abnormal"] = target == "已触发"
        return entry, f"限流规则已{action}"

    # ------------------------------------------------------------------ 窗口与回执
    def _active_rule(self, rule: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if _text(row.get("规则编号")) == rule:
                return row
        if rule.isdigit():
            return store.find(MODULE, int(rule))
        return None

    def _new_window(self, entry: dict[str, Any], now: datetime, window_no: int) -> dict[str, Any]:
        recovery = int(entry.get("恢复时间") or RECOVERY_MIN)
        limit = float(entry.get("每秒上限") or LIMIT_MIN)
        burst = float(entry.get("突发容量") or BURST_MIN)
        window = {
            "id": _next_id(store.rows(WINDOW_MODULE)),
            "规则编号": _text(entry.get("规则编号")),
            "窗口序号": window_no,
            "窗口开始": _iso(now),
            "窗口结束": _iso(now + timedelta(seconds=recovery)),
            "已通过": 0,
            # 放行时刻用于固定窗口/滑动窗口的每秒额度核算，也是判定留痕。
            "放行时刻": [],
            # 令牌桶：新窗口只给每秒上限的额度，空闲时按每秒上限补充、最多积累到突发容量。
            # 这样同一限流对象在突发容量内可以短时通过，但无法在 1 秒内绕过每秒上限。
            "令牌": float(min(limit, burst)),
            "上次补充": _iso(now),
            "判定次数": 0,
            "状态": "计数中",
            **_snapshot(entry),
        }
        store.rows(WINDOW_MODULE).append(window)
        return window

    def _window_for(self, entry: dict[str, Any], now: datetime) -> dict[str, Any]:
        """取当前计数窗口；恢复时间到达后关闭旧窗口并开新窗口，从零重新计数。"""
        window = _latest_window(_text(entry.get("规则编号")))
        if window is None:
            return self._new_window(entry, now, 1)
        ends_at = _as_datetime(window.get("窗口结束"))
        if now >= ends_at:
            if _text(window.get("状态")) == "降级中":
                self._close_window(entry, window, now)
            else:
                window["状态"] = "已恢复"
            return self._new_window(entry, now, int(window.get("窗口序号", 1)) + 1)
        return window

    def _close_window(self, entry: dict[str, Any], window: dict[str, Any], now: datetime) -> None:
        """关闭到期窗口：回执置为已恢复，并把路由切回目标服务。"""
        rule_code = _text(entry.get("规则编号"))
        window["状态"] = "已恢复"
        for row in store.rows(RECEIPT_MODULE):
            if row.get("规则编号") == rule_code and row.get("窗口序号") == window.get("窗口序号"):
                row["状态"] = "已恢复"
                row["恢复完成时间"] = _iso(now)
                self._sync_route(entry, row, degrade=False, now=now)
        entry["status"] = "生效中"
        entry["规则状态"] = "生效中"
        entry["abnormal"] = False
        entry["pending"] = True
        entry["窗口状态"] = "已恢复"
        entry["最近判定"] = "恢复"
        entry["最近判定原因"] = "恢复时间到达，从新窗口重新计数"
        entry["最近判定时间"] = _iso(now)

    def _recent_passes(self, window: dict[str, Any], now: datetime) -> int:
        """按判定口径统计最近 1 秒已放行的次数。"""
        basis = _text(window.get("判定口径")) or "令牌桶"
        stamps = [_as_datetime(value) for value in window.get("放行时刻", [])]
        if basis == "固定窗口":
            current_second = int(now.timestamp())
            return sum(1 for moment in stamps if int(moment.timestamp()) == current_second)
        return sum(1 for moment in stamps if moment > now - timedelta(seconds=1))

    def _rate_available(self, window: dict[str, Any], now: datetime) -> bool:
        """本次请求是否仍在每秒额度内：令牌桶看令牌，固定/滑动窗口看最近一秒放行数。"""
        limit = int(window.get("每秒上限") or LIMIT_MIN)
        burst = int(window.get("突发容量") or BURST_MIN)
        if (_text(window.get("判定口径")) or "令牌桶") == "令牌桶":
            last = _as_datetime(window.get("上次补充"))
            elapsed = max((now - last).total_seconds(), 0.0)
            tokens = min(float(burst), float(window.get("令牌", 0)) + elapsed * limit)
            window["令牌"] = tokens
            window["上次补充"] = _iso(now)
            return tokens >= 1
        return self._recent_passes(window, now) < limit

    def _consume_rate(self, window: dict[str, Any], now: datetime) -> None:
        if (_text(window.get("判定口径")) or "令牌桶") == "令牌桶":
            window["令牌"] = max(float(window.get("令牌", 0)) - 1, 0.0)
        window.setdefault("放行时刻", []).append(_iso(now))

    def _window_receipt(self, rule_code: str, window_no: int) -> dict[str, Any] | None:
        for row in store.rows(RECEIPT_MODULE):
            if row.get("规则编号") == rule_code and row.get("窗口序号") == window_no:
                return row
        return None

    def _thresholds(self, window: dict[str, Any], now: datetime) -> dict[str, Any]:
        """把四个阈值口径摊平，触发回执里能看出当时各自的实际取值。"""
        basis = _text(window.get("判定口径")) or "令牌桶"
        tokens = float(window.get("令牌", 0))
        if basis == "固定窗口":
            basis_detail = {"口径": "本秒放行", "实际": self._recent_passes(window, now)}
        elif basis == "滑动窗口":
            basis_detail = {"口径": "最近1秒放行", "实际": self._recent_passes(window, now)}
        else:
            basis_detail = {"口径": "令牌余量", "实际": round(tokens, 2)}
        return {
            "限流阈值": {"阈值": window.get("限流阈值"), "实际": window.get("已通过")},
            "每秒上限": {"阈值": window.get("每秒上限"), **basis_detail},
            "突发容量": {"阈值": window.get("突发容量"), "实际": window.get("已通过")},
            "判定口径": basis,
        }

    def _open_receipt(
        self,
        entry: dict[str, Any],
        window: dict[str, Any],
        now: datetime,
        *,
        trigger: str,
        trigger_value: int,
        reason: str,
    ) -> dict[str, Any]:
        """同一窗口只生成一份降级回执；重复触发复用已有回执，不追加第二条。"""
        rule_code = _text(entry.get("规则编号"))
        window_no = int(window.get("窗口序号", 1))
        existing = self._window_receipt(rule_code, window_no)
        if existing is not None:
            return existing
        rows = store.rows(RECEIPT_MODULE)
        receipt_id = _next_id(rows)
        receipt = {
            "id": receipt_id,
            "回执编号": f"LRCP-{receipt_id:04d}",
            "规则编号": rule_code,
            "限流对象": entry.get("限流对象"),
            "窗口序号": window_no,
            "回执类型": "降级回执",
            "判定结果": "降级",
            "触发阈值": trigger,
            "触发阈值值": trigger_value,
            "触发阈值明细": self._thresholds(window, now),
            "触发原因": reason,
            **_snapshot(window),
            "窗口内已通过": window.get("已通过"),
            "触发时间": _iso(now),
            "恢复截止": _iso(_as_datetime(window.get("窗口结束"))),
            "状态": "生效中",
            "关联路由": _find_route_code(entry),
            "同步路由": "",
            "切换目标": "",
            "路由同步": "未同步",
        }
        rows.append(receipt)
        return receipt

    def _sync_route(
        self,
        entry: dict[str, Any],
        receipt: dict[str, Any] | None,
        *,
        degrade: bool,
        now: datetime,
    ) -> dict[str, Any] | None:
        """把判定结果同步给路由服务：降级切降级路由，恢复切回目标服务。"""
        route_code = _text(receipt.get("关联路由")) if receipt else ""
        route_code = route_code or _find_route_code(entry)
        route = _ROUTER_SERVICE.apply_limiter_decision(
            route_code=route_code,
            rule_code=_text(entry.get("规则编号")),
            degraded=degrade,
            at=_iso(now),
        )
        if receipt is not None:
            receipt["同步路由"] = route_code
            if route is None:
                receipt["路由同步"] = "未找到关联路由"
            elif _text(route.get("当前路由")):
                receipt["路由同步"] = "已同步"
            else:
                receipt["路由同步"] = "降级服务未配置"
            receipt["切换目标"] = _text(route.get("当前路由")) if route else ""
        return route

    def _record_check(
        self,
        entry: dict[str, Any],
        window: dict[str, Any],
        *,
        decision: str,
        reason: str,
        now: datetime,
        trigger: str = "",
        trigger_value: int | None = None,
        receipt: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        rows = store.rows(CHECK_MODULE)
        check_id = _next_id(rows)
        check = {
            "id": check_id,
            "判定编号": f"LCHK-{check_id:04d}",
            "规则编号": _text(entry.get("规则编号")),
            "限流对象": entry.get("限流对象"),
            "窗口序号": window.get("窗口序号"),
            "判定结果": decision,
            "判定原因": reason,
            "触发阈值": trigger,
            "触发阈值值": trigger_value,
            "触发阈值明细": self._thresholds(window, now),
            **_snapshot(window),
            "窗口内已通过": window.get("已通过"),
            "判定时间": _iso(now),
            "回执编号": receipt.get("回执编号") if receipt else None,
        }
        rows.append(check)
        return check

    # ------------------------------------------------------------------ 对外判定
    def check(self, rule: str, *, at: Any = None) -> tuple[dict[str, Any] | None, str, dict[str, Any] | None]:
        """对同一限流对象做一次判定，返回 (判定流水, 说明, 回执)。

        放行条件：已降级窗口之外，每秒额度未用满，且累计放行未达到限流阈值/突发容量。
        触发条件：每秒额度用满、累计放行达到限流阈值或达到突发容量；同一窗口重复触发
        只复用第一份回执；恢复时间到达后自动进入新窗口并重新计数。
        """
        try:
            now = _parse_at(at) or datetime.now(timezone.utc)
        except ValueError as exc:
            return None, str(exc), None

        with _LOCK:
            entry = self._active_rule(_text(rule))
            if entry is None:
                return None, f"限流规则 {rule} 不存在或已归档", None
            status = _text(entry.get("status"))
            if status == "草稿":
                return None, f"限流规则 {entry.get('规则编号')} 尚未启用，不能参与判定", None
            if status == "已关闭":
                return None, f"限流规则 {entry.get('规则编号')} 已关闭，不再参与判定", None

            rule_code = _text(entry.get("规则编号"))
            window = self._window_for(entry, now)
            try:
                limit = int(window.get("每秒上限"))
                threshold = int(window.get("限流阈值"))
                basis = _text(window.get("判定口径")) or "令牌桶"
            except (TypeError, ValueError):
                return None, f"限流规则 {entry.get('规则编号')} 的阈值配置不完整，请先修正", None

            passed = int(window.get("已通过", 0))
            window["判定次数"] = int(window.get("判定次数", 0)) + 1
            # 触发前先把最近一秒的放行数定格，回执里的阈值明细要用这一刻的值。
            activity = self._thresholds(window, now)

            trigger = ""
            trigger_value: int | None = None
            receipt: dict[str, Any] | None = None

            if _text(window.get("状态")) == "降级中":
                existing = self._window_receipt(rule_code, int(window.get("窗口序号", 1)))
                decision = "降级"
                trigger = _text(existing.get("触发阈值")) if existing else "限流阈值"
                trigger_value = existing.get("触发阈值值") if existing else threshold
                reason = (
                    f"窗口已降级（回执 {existing.get('回执编号')}），沿用当时阈值，不再追加回执"
                    if existing else "窗口已降级，不再放行"
                )
                receipt = self._open_receipt(
                    entry, window, now,
                    trigger=trigger or "限流阈值",
                    trigger_value=int(trigger_value or threshold),
                    reason=reason,
                )
            else:
                # 先看每秒额度，再看窗口累计阈值：突发额度只在容量内短时放行，
                # 超过每秒上限的那一次直接降级并生成回执。
                rate_ok = self._rate_available(window, now)
                threshold_hit = passed >= threshold
                if not rate_ok or threshold_hit:
                    if not rate_ok:
                        trigger, trigger_value = "每秒上限", limit
                        reason = f"最近 1 秒放行超过每秒上限 {limit}（判定口径：{basis}），触发降级"
                    else:
                        trigger, trigger_value = "限流阈值", threshold
                        reason = f"窗口内累计放行达到限流阈值 {threshold}（判定口径：{basis}），触发降级"
                    decision = "降级"
                    receipt = self._open_receipt(
                        entry, window, now,
                        trigger=trigger, trigger_value=trigger_value, reason=reason,
                    )
                else:
                    self._consume_rate(window, now)
                    window["已通过"] = passed + 1
                    decision = "放行"
                    reason = f"窗口内第 {passed + 1} 次请求，未达到限流阈值 {threshold}（判定口径：{basis}）"

            if decision == "降级":
                window["状态"] = "降级中"
                self._sync_route(entry, receipt, degrade=True, now=now)
                entry["status"] = "已触发"
                entry["规则状态"] = "已触发"
                entry["abnormal"] = True
                entry["pending"] = False
                entry["窗口状态"] = "降级中"
                entry["最近回执"] = receipt.get("回执编号") if receipt else None
            else:
                entry["窗口状态"] = "计数中"

            entry["当前窗口"] = int(window.get("窗口序号", 1))
            entry["最近判定"] = decision
            entry["最近判定原因"] = reason
            entry["最近判定时间"] = _iso(now)

            check = self._record_check(
                entry, window,
                decision=decision, reason=reason, now=now,
                trigger=trigger, trigger_value=trigger_value, receipt=receipt,
            )
            return check, reason, receipt

    def restore(self, rule: str, *, at: Any = None) -> tuple[dict[str, Any] | None, str, dict[str, Any] | None]:
        """恢复时间到达后从新窗口开始；未到时间不允许提前恢复。"""
        try:
            now = _parse_at(at) or datetime.now(timezone.utc)
        except ValueError as exc:
            return None, str(exc), None

        with _LOCK:
            entry = self._active_rule(_text(rule))
            if entry is None:
                return None, f"限流规则 {rule} 不存在或已归档", None
            rule_code = _text(entry.get("规则编号"))
            window = _latest_window(rule_code)
            if window is None:
                return None, f"限流规则 {rule_code} 还没有产生计数窗口", None
            if _text(window.get("状态")) != "降级中":
                return None, f"限流规则 {rule_code} 当前没有待恢复的降级窗口", None
            ends_at = _as_datetime(window.get("窗口结束"))
            if now < ends_at:
                return None, f"恢复时间未到（{ends_at.isoformat()}），不能提前恢复", None

            self._close_window(entry, window, now)
            # 恢复后的第一次判定从零开始计数；这里只开窗，不预占额度。
            new_window = self._new_window(entry, now, int(window.get("窗口序号", 1)) + 1)
            entry["当前窗口"] = int(new_window.get("窗口序号", 1))
            check = self._record_check(
                entry, new_window,
                decision="恢复",
                reason="恢复时间到达，从新窗口重新计数",
                now=now,
            )
            return check, f"恢复时间已到，从第 {new_window.get('窗口序号')} 个窗口重新计数", None

    # ------------------------------------------------------------------ 查询明细
    def list_receipts(self, rule_code: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(RECEIPT_MODULE)
        if rule_code:
            rows = [row for row in rows if row.get("规则编号") == rule_code]
        return list(reversed(rows))

    def list_checks(self, rule_code: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(CHECK_MODULE)
        if rule_code:
            rows = [row for row in rows if row.get("规则编号") == rule_code]
        return list(reversed(rows))

    def list_windows(self, rule_code: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(WINDOW_MODULE)
        if rule_code:
            rows = [row for row in rows if row.get("规则编号") == rule_code]
        return list(reversed(rows))
