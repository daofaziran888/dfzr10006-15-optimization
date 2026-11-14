"""查询路由分发业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "router"
REQUIRED_FIELDS = ["规则编号", "匹配条件", "目标服务"]
STATUS_ORDER = ["草稿", "生效中", "已降级", "已关闭"]
ACTION_RULES = {"启用规则": "草稿", "降级路由": "生效中", "关闭规则": "已降级"}
NEGATIVE_ACTIONS = []


class RouterService:
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

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"路由规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于查询路由分发可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"路由规则已{action}"

    def list_by_code(self, rule_code: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if str(row.get("规则编号") or "").strip() == rule_code:
                return row
        return None

    def apply_limiter_decision(
        self,
        *,
        route_code: str,
        rule_code: str,
        degraded: bool,
        at: str | None = None,
    ) -> dict[str, Any] | None:
        """接收限流判定结果：降级时切到降级服务，恢复时切回目标服务。

        路由状态只在这里流转，限流服务不直接改路由字段，保证同步口径单一。
        """
        route = self.list_by_code(route_code)
        if route is None:
            return None
        target = str(route.get("降级服务") or "").strip() if degraded else str(route.get("目标服务") or "").strip()
        route["当前路由"] = target
        route["路由状态"] = "降级" if degraded else "正常"
        route["关联限流"] = rule_code
        route["生效中限流"] = rule_code if degraded else ""
        route["最近切换"] = at
        current_status = str(route.get("status") or "").strip()
        if degraded and current_status != "已关闭":
            route["status"] = "已降级"
            route["规则状态"] = "已降级"
            route["abnormal"] = True
            route["pending"] = False
        elif not degraded and current_status == "已降级":
            route["status"] = "生效中"
            route["规则状态"] = "生效中"
            route["abnormal"] = False
            route["pending"] = True
        return route
