"""快照计划服务：管理数仓表的定期快照策略，定义周期、保留份数和自动清理。

增强能力：
- 消费 deploy.published 通道，按「精确名称 / 通配模板」匹配目标，为发布后的
  模型生成带归属证明（模型身份+版本+字段结构）的版本快照
- 快照登记、错误挂接修复与回滚入口核对同一份归属证明，禁止按子串/前缀把
  dws_sales 的快照挂到 dws_sales_extra（或反向覆盖）
- 改名登记沿用同一模型身份，旧快照仍挂原名称，两个名称不会被当作同一张表
- 恢复按模型身份加锁，防止并发恢复覆盖同名前缀表的分区
- 过期清理策略：按保留份数自动清理过期分区
"""
from __future__ import annotations

import fnmatch
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.snapshot_identity import normalize_name
from app.store import store

VALID_CYCLES = {"每分钟", "每小时", "每次变更", "每日", "每周", "每月", "每季度"}
PARTITION_FORMATS = {"ds=yyyyMMdd", "hour=yyyyMMddHH", "week=yyyyWW", "version=yyyyMMddHHmmss",
                     "year=yyyy", "month=yyyyMM"}


def plan_matches_target(plan_target: str, event_target: str) -> bool:
    """计划目标与事件目标匹配：精确相等或通配模板（* ? [）命中。

    不再使用子串包含判断——``dws_sales`` 不得匹配 ``dws_sales_extra``。
    """
    plan = normalize_name(plan_target)
    event = normalize_name(event_target)
    if not plan or not event:
        return False
    if plan == event:
        return True
    if any(ch in plan for ch in "*?["):
        return fnmatch.fnmatchcase(event, plan)
    return False


class SnapshotPlanService(BaseModuleService):
    MODULE = "snapshot_plan"
    ENTITY = "快照策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略名称", "目标表", "快照周期", "保留份数", "分区格式", "增量标记", "过期清理"]
    REQUIRED_FIELDS = ["策略名称", "目标表", "快照周期"]
    STATUS_ORDER = ["草稿", "已启用", "已暂停"]
    ACTION_RULES = {"启用策略": "草稿", "暂停策略": "已启用"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 登记：创建快照计划
    # ------------------------------------------------------------------

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建快照策略：消费deploy.published通道，为目标表自动生成快照策略。"""
        target_table = normalize_name(values.get("目标表"))
        values["目标表"] = target_table
        snapshot_cycle = str(values.get("快照周期", "")).strip()

        # 标准化快照周期
        if snapshot_cycle not in VALID_CYCLES:
            snapshot_cycle = "每日"
            values["快照周期"] = snapshot_cycle

        # 标准化分区格式
        partition_fmt = str(values.get("分区格式", "")).strip()
        if partition_fmt not in PARTITION_FORMATS:
            partition_fmt = _infer_partition_format(snapshot_cycle)
            values["分区格式"] = partition_fmt

        # 自动计算保留份数
        if not values.get("保留份数"):
            values["保留份数"] = _calculate_retention(snapshot_cycle)

        # 登记计划目标的模型身份（具体表名才登记，通配模板不绑定单一身份）
        identity: dict[str, Any] | None = None
        if target_table and not any(ch in target_table for ch in "*?["):
            identity = store.snapshot.register_model(
                target_table,
                model_version=str(values.get("模型版本") or "v1.0"),
                fields=_coerce_fields(values.get("字段结构")),
            )
            values["_target_model_id"] = identity["model_id"]

        # 消费部署发布事件：仅精确/通配匹配的计划才能挂接发布快照
        deploy_msgs = store.channel.consume("deploy.published", "snapshot_plan")
        ownership_rejections: list[dict[str, Any]] = []
        for msg in deploy_msgs:
            event_target = normalize_name(msg["payload"].get("target_model"))
            if not plan_matches_target(target_table, event_target):
                # 子串/前缀相似但并非同一目标：记录拒绝，绝不挂接
                ownership_rejections.append({
                    "event_target": event_target,
                    "plan_target": target_table,
                    "reason": "target_not_match",
                })
                continue
            if any(ch in target_table for ch in "*?["):
                # 通配计划命中具体发布事件：以事件目标登记真实身份
                event_identity = store.snapshot.register_model(
                    event_target,
                    model_version=msg["payload"].get("source_version", "v1.0"),
                )
            else:
                event_identity = identity
            snap_data = {
                "target_model": event_target,
                "source_version": msg["payload"].get("source_version", "v1.0"),
                "deployed_at": msg["payload"].get("deployed_at"),
                "environment": msg["payload"].get("environment"),
                "partitions": list(msg["payload"].get("partitions") or []),
            }
            plan_fields = _coerce_fields(values.get("字段结构"))
            snap_id = store.snapshot.create_version_snapshot(
                "snapshot_plan", msg["payload"]["entry_id"],
                snap_data,
                msg["payload"].get("source_version", "v1.0"),
                target_model=event_target,
                model_id=event_identity["model_id"] if event_identity else None,
                model_version=msg["payload"].get("source_version", "v1.0"),
                fields=plan_fields,
                field_fingerprint=None if plan_fields
                else (event_identity or {}).get("field_fingerprint"),
            )
            values["_auto_snapshot_id"] = snap_id
            values["增量标记"] = "是"

        if ownership_rejections:
            values["_ownership_rejections"] = ownership_rejections
            store.channel.publish("snapshot.ownership_rejected", {
                "plan_target": target_table,
                "rejections": ownership_rejections,
            })

        # 设置过期清理策略
        if not values.get("过期清理"):
            retention = values.get("保留份数", "30")
            values["过期清理"] = f"自动删除超过{retention}{'天' if '日' in snapshot_cycle else '版本'}数据"

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        store.channel.publish("snapshot.strategy_created", {
            "strategy_name": entry["策略名称"],
            "target_table": target_table,
            "cycle": snapshot_cycle,
            "retention": entry.get("保留份数"),
            "partition_format": partition_fmt,
            "target_model_id": entry.get("_target_model_id"),
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "snapshot_plan", "snapshot_strategy",
            f"snapshot_{entry['id']}",
            {
                "strategy_name": entry["策略名称"],
                "target_table": target_table,
                "cycle": snapshot_cycle,
                "target_model_id": entry.get("_target_model_id"),
            },
            source_modules=["deploy_pipeline", "snapshot_plan"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return None, message

        target_table = normalize_name(entry.get("目标表"))

        if action == "启用策略":
            # 创建初始快照（带归属证明与分区清单）
            cycle = entry.get("快照周期", "每日")
            initial_snap = _create_initial_snapshot(entry)
            store.channel.publish("snapshot.enabled", {
                "strategy_name": entry["策略名称"],
                "target_table": target_table,
                "cycle": cycle,
                "initial_snapshot": initial_snap,
                "entry_id": entry_id,
            })

            # 注册恢复检查点：状态内固化模型身份，恢复时凭此核对
            checkpoint_state: dict[str, Any] = {
                "target_table": target_table,
                "cycle": cycle,
                "phase": "enabled",
            }
            identity = store.snapshot.resolve_identity(target_table)
            checkpoint_state.update(identity)
            cp_id = store.recovery.create_checkpoint(
                "snapshot_plan", f"snap_{entry_id}", checkpoint_state)
            store.recovery.mark_completed(cp_id)

        elif action == "暂停策略":
            store.channel.publish("snapshot.paused", {
                "strategy_name": entry["策略名称"],
                "target_table": target_table,
                "paused_at": datetime.now(timezone.utc).isoformat(),
                "entry_id": entry_id,
            })

        return entry, message

    # ------------------------------------------------------------------
    # 模型身份登记与改名
    # ------------------------------------------------------------------

    def register_target_identity(self, target_model: str, *, model_version: str = "v1.0",
                                 fields: list[str] | None = None) -> dict[str, Any]:
        """登记具体目标表的模型身份与字段结构。"""
        return store.snapshot.register_model(
            target_model, model_version=model_version, fields=_coerce_fields(fields))

    def register_target_rename(self, old_name: str, new_name: str, *,
                               model_version: str = "v1.0",
                               fields: list[str] | None = None) -> dict[str, Any]:
        """登记真正改名：同一模型身份绑定新旧名称，旧快照仍挂原名称。"""
        return store.snapshot.register_rename(
            old_name, new_name, model_version=model_version, fields=_coerce_fields(fields))

    # ------------------------------------------------------------------
    # 回滚入口：登记核对同一份归属证明
    # ------------------------------------------------------------------

    def list_target_snapshots(self, target_model: str, *,
                              model_version: str | None = None) -> dict[str, Any]:
        """从快照计划列表中挑选指定目标的恢复版本；名称相近的其他表不返回。"""
        identity = store.snapshot.resolve_identity(target_model)
        snaps = store.snapshot.list_snapshots_by_target(
            target_model, model_id=identity["model_id"], model_version=model_version)
        return {
            "target_model": identity["target_model"],
            "model_id": identity["model_id"],
            "total": len(snaps),
            "items": [
                {
                    "snap_id": s["snap_id"],
                    "version_label": s["version_label"],
                    "ownership": s["ownership"],
                    "partitions": s.get("data", {}).get("partitions", []),
                    "created_at": s["created_at"],
                }
                for s in snaps
            ],
        }

    def list_available_partitions(self, target_model: str, *,
                                  model_version: str | None = None) -> dict[str, Any]:
        """回滚入口核对归属证明后，列出该目标当前可用的分区。"""
        identity = store.snapshot.resolve_identity(target_model)
        partitions = store.snapshot.available_partitions(
            target_model, model_id=identity["model_id"], model_version=model_version)
        return {
            "target_model": identity["target_model"],
            "model_id": identity["model_id"],
            "total": len(partitions),
            "partitions": partitions,
        }

    def restore_target(self, target_model: str, snap_id: str, *,
                       partitions: list[str] | None = None,
                       model_version: str | None = None,
                       fields: list[str] | None = None,
                       lock_token: str | None = None) -> dict[str, Any]:
        """回滚任务：从计划中挑选指定目标的恢复版本并恢复分区。

        归属证明（模型身份/版本/字段结构）核对失败时不写任何分区；
        同一模型身份的并发恢复由恢复锁拒绝。未显式提供字段结构时不做
        结构比对，仅按模型身份与版本核对。
        """
        coerced_fields = _coerce_fields(fields)
        identity = store.snapshot.resolve_identity(
            target_model, model_version=model_version, fields=coerced_fields)
        return store.snapshot.restore_target_snapshot(
            target_model=target_model,
            snap_id=snap_id,
            partitions=partitions,
            model_id=identity["model_id"],
            model_version=model_version,
            fields=coerced_fields or None,
            lock_token=lock_token,
        )

    # ------------------------------------------------------------------
    # 错误挂接发现与修复
    # ------------------------------------------------------------------

    def find_misattachments(self, target_model: str) -> dict[str, Any]:
        """按原目标名称找出错误挂接的快照（精确匹配，不波及前缀相似表）。"""
        findings = store.snapshot.find_misattached(target_model)
        return {"target_model": normalize_name(target_model),
                "total": len(findings), "items": findings}

    def repair_target_attachment(self, snap_id: str, expected_target: str, *,
                                 model_version: str | None = None,
                                 fields: list[str] | None = None,
                                 reason: str = "manual_repair") -> dict[str, Any]:
        """修复错误挂接：核对真实身份后只改指定快照归属，不覆盖其他表历史。"""
        identity = store.snapshot.resolve_identity(
            expected_target, model_version=model_version, fields=_coerce_fields(fields))
        return store.snapshot.repair_attachment(
            snap_id,
            expected_target=expected_target,
            model_id=identity["model_id"],
            model_version=model_version,
            fields=_coerce_fields(fields),
            reason=reason,
        )

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 快照周期分布
        cycle_dist: dict[str, int] = {}
        for row in store.rows(self.MODULE):
            c = row.get("快照周期", "未指定")
            cycle_dist[c] = cycle_dist.get(c, 0) + 1
        base["cycle_distribution"] = cycle_dist

        # 增量快照统计
        incremental = sum(1 for row in store.rows(self.MODULE) if row.get("增量标记") == "是")
        base["incremental_snapshots"] = incremental

        # 消费的部署发布事件
        base["consumed_deploys"] = len(store.channel.consume("deploy.published", "snapshot_plan"))

        # 自动快照与归属治理统计
        snapshot_info = store.snapshot.snapshot_summary()
        base["auto_snapshots"] = snapshot_info.get("total_snapshots", 0)
        base["registered_target_models"] = snapshot_info.get("registered_models", 0)
        base["restore_records"] = snapshot_info.get("restore_records", 0)
        base["repair_records"] = snapshot_info.get("repair_records", 0)
        return base


def _coerce_fields(fields: Any) -> list[str]:
    """字段结构允许传列表或逗号/点分隔字符串，统一成有序去重列表。"""
    if not fields:
        return []
    if isinstance(fields, (list, tuple, set)):
        return sorted({str(f).strip() for f in fields if str(f).strip()})
    return sorted({f.strip() for f in str(fields).replace(".", ",").split(",") if f.strip()})


def _infer_partition_format(cycle: str) -> str:
    """根据快照周期推断分区格式。"""
    mapping = {"每小时": "hour=yyyyMMddHH", "每日": "ds=yyyyMMdd",
               "每周": "week=yyyyWW", "每月": "month=yyyyMM",
               "每次变更": "version=yyyyMMddHHmmss"}
    return mapping.get(cycle, "ds=yyyyMMdd")


def _calculate_retention(cycle: str) -> str:
    """根据快照周期计算合理的保留份数。"""
    mapping = {"每次变更": "365", "每小时": "168", "每日": "30",
               "每周": "52", "每月": "36", "每季度": "12"}
    return mapping.get(cycle, "30")


def _create_initial_snapshot(entry: dict[str, Any]) -> str:
    """为快照策略创建带归属证明的初始数据快照。"""
    target_table = normalize_name(entry.get("目标表"))
    identity = store.snapshot.resolve_identity(target_table)
    partition_fmt = str(entry.get("分区格式") or "ds=yyyyMMdd")
    initial_partition = f"{partition_fmt.split('=')[0]}={datetime.now(timezone.utc).strftime('%Y%m%d')}"
    snap_data = {
        "target_table": target_table,
        "target_model": target_table,
        "snapshot_version": "v1.0",
        "created_by": "snapshot_plan_enabled",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "partitions": [initial_partition],
    }
    return store.snapshot.create_snapshot(
        "snapshot_plan", entry.get("id"),
        snap_data,
        "init",
        target_model=target_table,
        model_id=identity["model_id"],
        model_version="v1.0",
        field_fingerprint=identity["field_fingerprint"],
    )
