"""调用限流降级接口：维护限流规则，执行限流判定、降级回执与恢复计数。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.limiter import BASIS_ORDER, LimiterService

router = APIRouter(prefix="/api/limiter", tags=["调用限流降级"])

service = LimiterService()

LIST_FIELDS = [
    "规则编号",
    "限流对象",
    "限流算法",
    "限流阈值",
    "每秒上限",
    "突发容量",
    "判定口径",
    "降级动作",
    "恢复时间",
    "规则状态",
]
STATUSES = ["草稿", "生效中", "已触发", "已关闭"]
BASES = BASIS_ORDER


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按规则编号检索"),
    status: str | None = Query(default=None, description="草稿、生效中、已触发、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按规则编号与状态过滤调用限流降级列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/rules", response_model=dict)
def rule_catalog() -> dict[str, Any]:
    """限流规则清单：字段口径、允许状态、判定口径与阈值边界一次返回给前端。"""
    return {
        "fields": LIST_FIELDS,
        "statuses": STATUSES,
        "basis": BASES,
        "required": [
            "规则编号", "限流对象", "限流算法", "每秒上限",
            "突发容量", "降级动作", "恢复时间",
        ],
        "defaults": {"限流阈值": "缺省等于每秒上限", "判定口径": "令牌桶"},
        "bounds": {
            "限流阈值": {"min": 1, "max": 1_000_000},
            "每秒上限": {"min": 1, "max": 1_000_000},
            "突发容量": {"min": 1, "max": 1_000_000},
            "恢复时间": {"min": 1, "max": 86_400, "unit": "秒"},
        },
        "order": ["每秒上限 <= 限流阈值 <= 突发容量", "每秒上限 <= 突发容量"],
        "total": len(service.list_entries(page=1, size=10000)[0]),
    }


@router.get("/receipts", response_model=dict)
def list_receipts(rule_code: str | None = Query(default=None, description="按规则编号过滤")) -> dict[str, Any]:
    """降级回执清单：每个触发窗口只保留一份，含触发当时的阈值快照。"""
    items = service.list_receipts(rule_code)
    return {"items": items, "total": len(items)}


@router.get("/checks", response_model=dict)
def list_checks(rule_code: str | None = Query(default=None, description="按规则编号过滤")) -> dict[str, Any]:
    """限流判定流水：每次放行或降级都保留当时的阈值口径。"""
    items = service.list_checks(rule_code)
    return {"items": items, "total": len(items)}


@router.get("/windows", response_model=dict)
def list_windows(rule_code: str | None = Query(default=None, description="按规则编号过滤")) -> dict[str, Any]:
    """计数窗口清单：恢复时间到达后自动开新窗口，历史窗口原样保留。"""
    items = service.list_windows(rule_code)
    return {"items": items, "total": len(items)}


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出调用限流降级清单：返回规则、回执与判定流水。"""
    items, total = service.list_entries(page=1, size=10000)
    return {
        "module": "limiter",
        "total": total,
        "items": items,
        "receipts": service.list_receipts(),
        "checks": service.list_checks(),
        "windows": service.list_windows(),
    }


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条限流规则；阈值越界或口径缺失时逐条说明原因，不静默保存。"""
    entry, errors = service.create_entry(payload.values)
    if errors:
        return ActionResult(ok=False, message="；".join(errors))
    return ActionResult(ok=True, message="限流规则已登记", entry=entry)


@router.put("/{entry_id}", response_model=ActionResult)
def update_entry(entry_id: int, payload: EntryPayload) -> ActionResult:
    """修改限流规则；只允许在阈值边界内保存，历史回执不受影响。"""
    entry, errors = service.update_entry(entry_id, payload.values)
    if errors:
        return ActionResult(ok=False, message="；".join(errors))
    return ActionResult(ok=True, message="限流规则已更新", entry=entry)


@router.post("/{entry_id}/checks", response_model=ActionResult)
def run_check(entry_id: int, payload: EntryPayload) -> ActionResult:
    """按规则做一次限流判定：同一限流对象在突发容量内放行，超出阈值生成降级回执。

    两次判定的先后由服务端串行执行，并发争抢最后一个突发额度时只会放行一个。
    可传 ``at``（ISO 8601）模拟跨窗口的时间，省略时按当前时间判定。
    """
    values = payload.values
    target = str(values.get("规则编号") or entry_id).strip()
    check, message, receipt = service.check(target, at=values.get("at"))
    if check is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=check, receipt=receipt)


@router.post("/{entry_id}/restore", response_model=ActionResult)
def restore(entry_id: int, payload: EntryPayload) -> ActionResult:
    """恢复时间到达后切回目标服务并开新窗口；未到时间会被拒绝。"""
    target = str(payload.values.get("规则编号") or entry_id).strip()
    check, message, _ = service.restore(target, at=payload.values.get("at"))
    if check is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=check)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条限流规则执行启用限流、触发降级、关闭限流；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条限流规则明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"限流规则 {entry_id} 不存在或已归档")
    return entry
