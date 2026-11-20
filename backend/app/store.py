"""内存数据仓库：跨模块数据通道、失败恢复、快照引擎和产物注册。

各业务模块产生的数据产物通过 DataChannel 发布，下游模块消费；
FailureRecovery 管理断点恢复；SnapshotEngine 提供版本快照能力。
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS
from app.snapshot_identity import (
    ConcurrentRestoreError,
    IdentityMismatchError,
    SchemaMismatchError,
    field_fingerprint as compute_field_fingerprint,
    identity_fingerprint,
    is_prefix_collision,
    model_id_for,
    normalize_name,
)


class DataChannel:
    """跨模块数据通道：上游模块发布数据产物，下游模块通过 topic 消费。"""

    def __init__(self) -> None:
        self._channels: dict[str, list[dict[str, Any]]] = {}
        self._consumers: dict[str, list[str]] = {}

    def publish(self, topic: str, payload: dict[str, Any]) -> str:
        """将数据产物发布到指定 topic，返回消息ID。"""
        msg_id = str(uuid.uuid4())[:12]
        entry = {
            "msg_id": msg_id,
            "topic": topic,
            "payload": payload,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "consumed_by": [],
            "expired": False,
        }
        self._channels.setdefault(topic, []).append(entry)
        return msg_id

    def consume(self, topic: str, consumer: str) -> list[dict[str, Any]]:
        """消费指定 topic 上未被当前消费者消费过的消息，标记已消费。"""
        channel = self._channels.get(topic, [])
        results: list[dict[str, Any]] = []
        for entry in channel:
            if consumer not in entry["consumed_by"] and not entry["expired"]:
                entry["consumed_by"].append(consumer)
                results.append(entry)
        return results

    def consume_latest(self, topic: str, consumer: str) -> dict[str, Any] | None:
        """消费指定 topic 上最新一条未被消费的消息。"""
        channel = self._channels.get(topic, [])
        for entry in reversed(channel):
            if consumer not in entry["consumed_by"] and not entry["expired"]:
                entry["consumed_by"].append(consumer)
                return entry
        return None

    def topic_snapshot(self, topic: str) -> list[dict[str, Any]]:
        """获取 topic 上所有消息的快照。"""
        return list(self._channels.get(topic, []))

    def expire_topic(self, topic: str) -> int:
        """过期指定 topic 上所有未消费消息，返回过期数量。"""
        count = 0
        for entry in self._channels.get(topic, []):
            if not entry["expired"]:
                entry["expired"] = True
                count += 1
        return count

    def list_topics(self) -> list[str]:
        return sorted(self._channels)

    def topic_stats(self) -> dict[str, dict[str, int]]:
        stats: dict[str, dict[str, int]] = {}
        for topic, entries in self._channels.items():
            consumed = sum(1 for e in entries if len(e["consumed_by"]) > 0)
            pending = len(entries) - consumed
            expired = sum(1 for e in entries if e["expired"])
            stats[topic] = {"total": len(entries), "consumed": consumed, "pending": pending, "expired": expired}
        return stats


class FailureRecovery:
    """失败恢复管理器：检查点管理、重试计数和失败链路追踪。"""

    def __init__(self) -> None:
        self._checkpoints: dict[str, dict[str, Any]] = {}
        self._retry_logs: dict[str, list[dict[str, Any]]] = {}
        self._failure_chains: dict[str, list[dict[str, Any]]] = {}

    def create_checkpoint(self, module: str, task_id: str, state: dict[str, Any]) -> str:
        """创建处理检查点，返回检查点ID。"""
        cp_id = f"{module}-{task_id}-{uuid.uuid4().hex[:8]}"
        self._checkpoints[cp_id] = {
            "cp_id": cp_id,
            "module": module,
            "task_id": task_id,
            "state": dict(state),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "restored_at": None,
            "restore_count": 0,
            "completed": False,
        }
        return cp_id

    def get_checkpoint(self, cp_id: str) -> dict[str, Any] | None:
        return self._checkpoints.get(cp_id)

    def update_checkpoint(self, cp_id: str, state: dict[str, Any]) -> bool:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return False
        cp["state"].update(state)
        return True

    def mark_completed(self, cp_id: str) -> bool:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return False
        cp["completed"] = True
        return True

    def restore_checkpoint(self, cp_id: str) -> dict[str, Any] | None:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return None
        cp["restored_at"] = datetime.now(timezone.utc).isoformat()
        cp["restore_count"] += 1
        return dict(cp)

    def log_retry(self, module: str, task_id: str, attempt: int, reason: str, success: bool) -> None:
        self._retry_logs.setdefault(module, [])
        self._retry_logs[module].append({
            "task_id": task_id,
            "attempt": attempt,
            "reason": reason,
            "success": success,
            "logged_at": datetime.now(timezone.utc).isoformat(),
        })

    def retry_history(self, module: str) -> list[dict[str, Any]]:
        return self._retry_logs.get(module, [])

    def log_failure_chain(self, chain_id: str, step: str, module: str, error: str, upstream: str | None = None) -> None:
        self._failure_chains.setdefault(chain_id, [])
        self._failure_chains[chain_id].append({
            "step": step,
            "module": module,
            "error": error,
            "upstream": upstream,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def chain_summary(self, chain_id: str) -> list[dict[str, Any]]:
        return self._failure_chains.get(chain_id, [])

    def recovery_summary(self) -> dict[str, Any]:
        completed = sum(1 for cp in self._checkpoints.values() if cp["completed"])
        pending = len(self._checkpoints) - completed
        total_retries = sum(len(logs) for logs in self._retry_logs.values())
        total_chains = len(self._failure_chains)
        return {
            "checkpoints": {"total": len(self._checkpoints), "completed": completed, "pending": pending},
            "retries": total_retries,
            "failure_chains": total_chains,
        }


class SnapshotEngine:
    """版本快照引擎：创建、比较和管理数据快照。

    每个快照在创建时即固化「归属证明」（target_model 名称 + model_id 模型
    身份 + model_version 版本 + field_fingerprint 字段结构），登记、修复与
    回滚共用同一份证明；名称相近（如 dws_sales / dws_sales_extra）的两张表
    不得共享归属。恢复入口凭模型身份与字段结构校验恢复范围，并对同一模型
    身份加锁，防止并发恢复覆盖另一张同名前缀表的分区。
    """

    def __init__(self) -> None:
        self._snapshots: dict[str, dict[str, Any]] = {}
        self._diff_logs: dict[str, dict[str, Any]] = {}
        # 模型登记表：model_id -> 规范名称/版本/字段/别名
        self._model_registry: dict[str, dict[str, Any]] = {}
        # 名称 -> model_id，改名后新旧名称都指向同一身份
        self._name_index: dict[str, str] = {}
        # 恢复锁：model_id -> 持锁令牌
        self._restore_locks: dict[str, str] = {}
        # 恢复审计记录、归属修复记录、已恢复分区台账
        self._restore_log: list[dict[str, Any]] = []
        self._repair_log: list[dict[str, Any]] = []
        self._restored_partitions: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 模型身份登记与改名
    # ------------------------------------------------------------------

    def register_model(self, target_model: str, *, model_version: str = "v1.0",
                       fields: list[str] | None = None) -> dict[str, Any]:
        """登记目标模型身份与字段结构，返回规范化身份信息。

        未提供字段结构时保留该版本已固化的指纹，不用空结构覆盖历史登记。
        """
        name = normalize_name(target_model)
        if not name:
            raise ValueError("目标模型名称不能为空")
        model_id = self._name_index.get(name) or model_id_for(name)
        fp = compute_field_fingerprint(fields)
        record = self._model_registry.setdefault(model_id, {
            "model_id": model_id,
            "canonical_name": name,
            "aliases": [],
            "versions": {},
        })
        existing_fp = record["versions"].get(model_version)
        if not fields and existing_fp:
            fp = existing_fp
        record["versions"][model_version] = fp
        record["aliases"] = sorted(set(record.get("aliases", [])) | {name})
        self._name_index[name] = model_id
        return {"target_model": name, "model_id": model_id,
                "model_version": model_version, "field_fingerprint": fp}

    def register_rename(self, old_name: str, new_name: str, *,
                        model_version: str = "v1.0",
                        fields: list[str] | None = None) -> dict[str, Any]:
        """登记真正的改名：新旧名称指向同一 model_id；旧快照仍挂在原名称。"""
        old, new = normalize_name(old_name), normalize_name(new_name)
        if not old or not new:
            raise ValueError("改名前后的名称均不能为空")
        if old == new:
            raise ValueError("新旧名称相同，不构成改名")
        existing_new = self._name_index.get(new)
        old_id = self._name_index.get(old)
        if existing_new and old_id and existing_new != old_id:
            raise ValueError(f"名称 {new} 已属于另一模型身份，禁止改名合并")
        identity = self.register_model(old, model_version=model_version, fields=fields)
        record = self._model_registry[identity["model_id"]]
        record["canonical_name"] = new
        record["aliases"] = sorted(set(record.get("aliases", [])) | {old, new})
        self._name_index[new] = identity["model_id"]
        self._name_index[old] = identity["model_id"]
        return {**identity, "target_model": new, "previous_name": old,
                "aliases": list(record["aliases"])}

    def resolve_identity(self, target_model: str, *, model_version: str | None = None,
                         fields: list[str] | None = None) -> dict[str, Any]:
        """解析目标名称对应的模型身份；未登记时按名称稳定派生。

        未显式提供字段结构时，回退到登记表中该版本（或最新版本）固化的
        字段指纹，保证后续创建的快照与登记结构一致。
        """
        name = normalize_name(target_model)
        model_id = self._name_index.get(name) or model_id_for(name)
        fp = compute_field_fingerprint(fields)
        record = self._model_registry.get(model_id)
        if record and model_version is None and record.get("versions"):
            model_version = sorted(record["versions"])[-1]
        if not fields and record and record.get("versions"):
            version_key = model_version if model_version in record["versions"] \
                else sorted(record["versions"])[-1]
            fp = record["versions"][version_key]
        return {"target_model": name, "model_id": model_id,
                "model_version": model_version or "v1.0", "field_fingerprint": fp}

    # ------------------------------------------------------------------
    # 快照创建（固化归属证明）
    # ------------------------------------------------------------------

    def _build_ownership(self, data: dict[str, Any], target_model: str | None,
                         model_id: str | None, model_version: str | None,
                         fields: list[str] | None, field_fp: str | None) -> dict[str, Any]:
        name = normalize_name(target_model or data.get("target_model") or data.get("target_table"))
        if not name:
            raise ValueError("快照必须携带目标表名才能固化归属")
        if fields is None:
            fields = list(data.get("fields") or [])
        fp = field_fp or compute_field_fingerprint(fields)
        resolved_id = model_id or self._name_index.get(name) or model_id_for(name)
        version = model_version or str(data.get("source_version") or data.get("model_version") or "v1.0")
        return {
            "target_model": name,
            "model_id": resolved_id,
            "model_version": version,
            "field_fingerprint": fp,
            "identity_fingerprint": identity_fingerprint(
                model_id=resolved_id, model_version=version, field_fp=fp),
        }

    def create_snapshot(self, module: str, entry_id: int, data: dict[str, Any],
                        version_label: str = "", *, target_model: str | None = None,
                        model_id: str | None = None, model_version: str | None = None,
                        fields: list[str] | None = None,
                        field_fingerprint: str | None = None) -> str:
        """创建数据快照并固化归属证明，返回快照ID。"""
        data_str = str(sorted(data.items()))
        content_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
        snap_id = f"{module}-{entry_id}-v{version_label}-{uuid.uuid4().hex[:8]}"
        ownership = self._build_ownership(
            data, target_model, model_id, model_version, fields, field_fingerprint)
        self._snapshots[snap_id] = {
            "snap_id": snap_id,
            "module": module,
            "entry_id": entry_id,
            "version_label": version_label,
            "data": dict(data),
            "content_hash": content_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "parent_snap_id": None,
            "retention_policy": "keep_forever",
            "ownership": ownership,
        }
        return snap_id

    def create_version_snapshot(self, module: str, entry_id: int, data: dict[str, Any],
                                version_label: str, parent_snap_id: str | None = None, *,
                                target_model: str | None = None,
                                model_id: str | None = None,
                                model_version: str | None = None,
                                fields: list[str] | None = None,
                                field_fingerprint: str | None = None) -> str:
        """创建带父子关系的版本快照，并固化目标模型归属证明。"""
        data_str = str(sorted(data.items()))
        content_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
        snap_id = f"{module}-{entry_id}-{version_label}-{uuid.uuid4().hex[:8]}"
        ownership = self._build_ownership(
            data, target_model, model_id,
            model_version or version_label, fields, field_fingerprint)
        self._snapshots[snap_id] = {
            "snap_id": snap_id,
            "module": module,
            "entry_id": entry_id,
            "version_label": version_label,
            "data": dict(data),
            "content_hash": content_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "parent_snap_id": parent_snap_id,
            "retention_policy": "keep_forever",
            "ownership": ownership,
        }
        return snap_id

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshots.get(snap_id)

    def list_snapshots(self, module: str, entry_id: int) -> list[dict[str, Any]]:
        return [
            s for s in self._snapshots.values()
            if s["module"] == module and s["entry_id"] == entry_id
        ]

    def list_snapshots_by_target(self, target_model: str, *,
                                 model_id: str | None = None,
                                 model_version: str | None = None) -> list[dict[str, Any]]:
        """按目标表精确（规范化后相等）列出快照；前缀相似的其他表不返回。"""
        name = normalize_name(target_model)
        expected_id = model_id or self._name_index.get(name)
        results = []
        for snap in self._snapshots.values():
            owner = snap.get("ownership") or {}
            if expected_id is not None:
                if owner.get("model_id") != expected_id:
                    continue
            elif normalize_name(owner.get("target_model")) != name:
                continue
            if model_version and owner.get("model_version") != model_version:
                continue
            results.append(snap)
        return sorted(results, key=lambda s: s["created_at"])

    # ------------------------------------------------------------------
    # 归属核对、错误挂接发现与修复
    # ------------------------------------------------------------------

    def find_misattached(self, target_model: str) -> list[dict[str, Any]]:
        """按原目标名称找出错误挂接：挂在同一计划条目下、归属却是别的模型。

        名称比较一律精确相等，``dws_sales`` 不会把 ``dws_sales_extra`` 的
        正确历史误判为错误挂接。
        """
        name = normalize_name(target_model)
        expected_id = self._name_index.get(name)
        grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for snap in self._snapshots.values():
            grouped.setdefault((snap["module"], snap["entry_id"]), []).append(snap)

        findings: list[dict[str, Any]] = []
        for snaps in grouped.values():
            names = {normalize_name(s.get("ownership", {}).get("target_model")) for s in snaps}
            if name not in names:
                continue
            registry_record = self._model_registry.get(expected_id or "")
            known_aliases = set(registry_record.get("aliases", [])) if registry_record else set()
            for snap in snaps:
                owner = snap.get("ownership", {})
                actual_name = normalize_name(owner.get("target_model"))
                if actual_name == name:
                    continue
                if expected_id and owner.get("model_id") == expected_id and actual_name in known_aliases:
                    continue  # 同一身份且为登记过的改名别名，不算错误挂接
                findings.append({
                    "snap_id": snap["snap_id"],
                    "module": snap["module"],
                    "entry_id": snap["entry_id"],
                    "expected_target": name,
                    "actual_target": actual_name,
                    "actual_model_id": owner.get("model_id"),
                    "model_version": owner.get("model_version"),
                    "field_fingerprint": owner.get("field_fingerprint"),
                    "prefix_collision": is_prefix_collision(name, actual_name),
                })
        return findings

    def verify_ownership(self, snap_id: str, *, target_model: str,
                         model_id: str | None = None,
                         model_version: str | None = None,
                         fields: list[str] | None = None,
                         field_fp: str | None = None) -> dict[str, Any]:
        """核对快照归属证明；身份或字段结构不符即拒绝并返回全部原因。"""
        snap = self._snapshots.get(snap_id)
        if snap is None:
            raise IdentityMismatchError(f"快照 {snap_id} 不存在", reasons=["snapshot_not_found"])
        owner = snap.get("ownership") or {}
        name = normalize_name(target_model)
        expected_id = model_id or self._name_index.get(name)
        reasons: list[str] = []
        if normalize_name(owner.get("target_model")) != name and (
                expected_id is None or owner.get("model_id") != expected_id):
            reasons.append(
                f"目标名称不符：快照归属 {owner.get('target_model')}，请求目标 {name}")
        if expected_id and owner.get("model_id") != expected_id:
            reasons.append(
                f"模型身份不符：快照 model_id={owner.get('model_id')}，请求 model_id={expected_id}")
        if model_version and owner.get("model_version") != model_version:
            reasons.append(
                f"模型版本不符：快照 {owner.get('model_version')}，请求 {model_version}")
        expected_fingerprint = field_fp or compute_field_fingerprint(fields)
        if fields is not None and owner.get("field_fingerprint") != expected_fingerprint:
            reasons.append(
                f"字段结构不符：快照 {owner.get('field_fingerprint')}，请求 {expected_fingerprint}")
        if reasons:
            if any("字段结构" in r for r in reasons):
                raise SchemaMismatchError("；".join(reasons), reasons=reasons)
            raise IdentityMismatchError("；".join(reasons), reasons=reasons)
        return {"snap_id": snap_id, "verified": True, "ownership": dict(owner),
                "identity_fingerprint": owner.get("identity_fingerprint")}

    def repair_attachment(self, snap_id: str, *, expected_target: str,
                          model_id: str | None = None,
                          model_version: str | None = None,
                          fields: list[str] | None = None,
                          reason: str = "manual_repair") -> dict[str, Any]:
        """修复错误挂接：仅重写指定快照的归属，不动其他表的正确历史。

        修复前必须确认快照真实身份与预期目标一致，否则拒绝，避免把
        ``dws_sales`` 的错误快照改写后覆盖 ``dws_sales_extra`` 的历史。
        """
        snap = self._snapshots.get(snap_id)
        if snap is None:
            raise IdentityMismatchError(f"快照 {snap_id} 不存在", reasons=["snapshot_not_found"])
        owner = dict(snap.get("ownership") or {})
        expected_name = normalize_name(expected_target)
        expected_id = model_id or self._name_index.get(expected_name)
        if expected_id is None:
            raise IdentityMismatchError(
                f"目标 {expected_name} 未登记模型身份，无法证明修复后的归属",
                reasons=["target_model_unregistered"])
        if owner.get("model_id") != expected_id:
            raise IdentityMismatchError(
                f"拒绝修复：快照真实身份 {owner.get('model_id')} 不属于 {expected_name}({expected_id})",
                reasons=["ownership_proof_mismatch"])
        if model_version and owner.get("model_version") != model_version:
            raise IdentityMismatchError(
                f"拒绝修复：版本 {owner.get('model_version')} 与请求 {model_version} 不一致",
                reasons=["model_version_mismatch"])
        new_fp = compute_field_fingerprint(fields)
        if fields is not None and owner.get("field_fingerprint") != new_fp:
            raise IdentityMismatchError(
                "拒绝修复：字段结构与快照固化结构不一致", reasons=["field_fingerprint_mismatch"])

        before = dict(owner)
        owner["target_model"] = expected_name
        owner["model_id"] = expected_id
        record = self._model_registry.get(expected_id, {})
        aliases = set(record.get("aliases", [])) | {expected_name, before.get("target_model", "")}
        record["aliases"] = sorted(a for a in aliases if a)
        snap["ownership"] = owner
        snap.setdefault("repair_history", []).append({
            "from_target": before.get("target_model"),
            "to_target": expected_name,
            "model_id": expected_id,
            "reason": reason,
            "repaired_at": datetime.now(timezone.utc).isoformat(),
        })
        repair_record = {
            "snap_id": snap_id,
            "module": snap["module"],
            "entry_id": snap["entry_id"],
            "before": before,
            "after": dict(owner),
            "reason": reason,
            "repaired_at": datetime.now(timezone.utc).isoformat(),
        }
        self._repair_log.append(repair_record)
        return repair_record

    def repair_history(self) -> list[dict[str, Any]]:
        return list(self._repair_log)

    # ------------------------------------------------------------------
    # 并发恢复锁与带归属校验的分区恢复
    # ------------------------------------------------------------------

    def acquire_restore_lock(self, model_id: str, token: str | None = None) -> str:
        """对同一模型身份加恢复锁；已被其他任务持锁时拒绝并发恢复。"""
        holder = self._restore_locks.get(model_id)
        if holder is not None and holder != token:
            raise ConcurrentRestoreError(
                f"模型 {model_id} 已有恢复任务 {holder} 持锁",
                reasons=["restore_lock_held"])
        token = token or f"restore-{uuid.uuid4().hex[:10]}"
        self._restore_locks[model_id] = token
        return token

    def release_restore_lock(self, model_id: str, token: str) -> bool:
        if self._restore_locks.get(model_id) == token:
            self._restore_locks.pop(model_id, None)
            return True
        return False

    def restore_target_snapshot(self, *, target_model: str, snap_id: str,
                                partitions: list[str] | None = None,
                                model_id: str | None = None,
                                model_version: str | None = None,
                                fields: list[str] | None = None,
                                lock_token: str | None = None) -> dict[str, Any]:
        """回滚入口：凭归属证明校验恢复范围后才允许恢复分区。

        - 身份/版本/字段结构任一不符：拒绝且不写任何分区；
        - 同一 model_id 已有恢复任务持锁：拒绝并发恢复；
        - 恢复记录固化快照归属证明，可审计真实归属。
        """
        proof = self.verify_ownership(
            snap_id, target_model=target_model, model_id=model_id,
            model_version=model_version, fields=fields)
        owner = proof["ownership"]
        token = self.acquire_restore_lock(owner["model_id"], lock_token)
        snap = self._snapshots[snap_id]
        requested = [str(p).strip() for p in (partitions or []) if str(p).strip()]
        available = snap.get("data", {}).get("partitions") or []
        if requested:
            invalid = [p for p in requested if p not in available]
            if invalid:
                if lock_token is None:
                    self.release_restore_lock(owner["model_id"], token)
                raise IdentityMismatchError(
                    f"分区 {invalid} 不在快照 {snap_id} 的恢复范围内",
                    reasons=["partition_out_of_scope"])
            scoped_partitions = requested
        else:
            scoped_partitions = list(available)
        record = {
            "restore_id": f"restore-{uuid.uuid4().hex[:10]}",
            "snap_id": snap_id,
            "target_model": normalize_name(target_model),
            "model_id": owner["model_id"],
            "model_version": owner["model_version"],
            "field_fingerprint": owner["field_fingerprint"],
            "identity_fingerprint": owner["identity_fingerprint"],
            "partitions": scoped_partitions,
            "lock_token": token,
            "restored_at": datetime.now(timezone.utc).isoformat(),
            "status": "restored",
        }
        self._restore_log.append(record)
        for part in scoped_partitions:
            self._restored_partitions.append({
                "partition": part,
                "target_model": record["target_model"],
                "model_id": owner["model_id"],
                "snap_id": snap_id,
                "restore_id": record["restore_id"],
            })
        if lock_token is None:
            self.release_restore_lock(owner["model_id"], token)
        return record

    def available_partitions(self, target_model: str, *, model_id: str | None = None,
                             model_version: str | None = None) -> list[dict[str, Any]]:
        """回滚入口查询：列出通过归属证明、当前可用于该目标的分区。"""
        result = []
        for snap in self.list_snapshots_by_target(
                target_model, model_id=model_id, model_version=model_version):
            for part in snap.get("data", {}).get("partitions", []):
                result.append({
                    "partition": part,
                    "snap_id": snap["snap_id"],
                    "target_model": snap["ownership"]["target_model"],
                    "model_id": snap["ownership"]["model_id"],
                    "model_version": snap["ownership"]["model_version"],
                    "identity_fingerprint": snap["ownership"]["identity_fingerprint"],
                })
        return result

    def restore_history(self, target_model: str | None = None) -> list[dict[str, Any]]:
        name = normalize_name(target_model)
        if not name:
            return list(self._restore_log)
        return [r for r in self._restore_log if r["target_model"] == name]

    def restored_partitions(self, target_model: str | None = None) -> list[dict[str, Any]]:
        name = normalize_name(target_model)
        if not name:
            return list(self._restored_partitions)
        return [r for r in self._restored_partitions if r["target_model"] == name]

    def diff_snapshots(self, snap_id_a: str, snap_id_b: str) -> dict[str, Any] | None:
        snap_a = self._snapshots.get(snap_id_a)
        snap_b = self._snapshots.get(snap_id_b)
        if not snap_a or not snap_b:
            return None
        keys_a = set(snap_a["data"])
        keys_b = set(snap_b["data"])
        added = {k: snap_b["data"][k] for k in keys_b - keys_a}
        removed = {k: snap_a["data"][k] for k in keys_a - keys_b}
        modified = {}
        for k in keys_a & keys_b:
            if snap_a["data"][k] != snap_b["data"][k]:
                modified[k] = {"old": snap_a["data"][k], "new": snap_b["data"][k]}
        diff_id = f"diff-{uuid.uuid4().hex[:8]}"
        result = {
            "diff_id": diff_id,
            "snap_a": snap_id_a,
            "snap_b": snap_id_b,
            "added_fields": added,
            "removed_fields": removed,
            "modified_fields": modified,
            "total_changes": len(added) + len(removed) + len(modified),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._diff_logs[diff_id] = result
        return result

    def rollback_to_snapshot(self, module: str, entry_id: int, snap_id: str,
                             rows: list[dict[str, Any]], *,
                             expected_target: str | None = None) -> dict[str, Any] | None:
        """将数据回滚到指定快照版本。

        快照带归属证明且给出 expected_target 时，先核对归属；名称相近但
        身份不同的快照直接拒绝，避免覆盖另一张同名前缀表的数据。
        """
        snap = self._snapshots.get(snap_id)
        if not snap:
            return None
        owner = snap.get("ownership")
        if owner and expected_target:
            self.verify_ownership(snap_id, target_model=expected_target)
        for row in rows:
            if row.get("id") == entry_id:
                for k, v in snap["data"].items():
                    row[k] = v
                row["_rollback_to"] = snap_id
                row["_rollback_at"] = datetime.now(timezone.utc).isoformat()
                return row
        return None

    def snapshot_summary(self) -> dict[str, Any]:
        by_module: dict[str, int] = {}
        for s in self._snapshots.values():
            by_module[s["module"]] = by_module.get(s["module"], 0) + 1
        return {
            "total_snapshots": len(self._snapshots),
            "by_module": by_module,
            "total_diffs": len(self._diff_logs),
            "registered_models": len(self._model_registry),
            "restore_records": len(self._restore_log),
            "repair_records": len(self._repair_log),
        }


class ArtifactRegistry:
    """数据产物注册表：追踪各模块产出的数据工件。"""

    def __init__(self) -> None:
        self._artifacts: dict[str, dict[str, Any]] = {}

    def register(self, module: str, artifact_type: str, artifact_id: str,
                 metadata: dict[str, Any], source_modules: list[str] | None = None) -> str:
        """注册数据产物，返回注册ID。"""
        reg_id = f"{module}-{artifact_type}-{artifact_id}"
        self._artifacts[reg_id] = {
            "reg_id": reg_id,
            "module": module,
            "artifact_type": artifact_type,
            "artifact_id": artifact_id,
            "metadata": dict(metadata),
            "source_modules": source_modules or [module],
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "consumed_by": [],
        }
        return reg_id

    def get_artifact(self, reg_id: str) -> dict[str, Any] | None:
        return self._artifacts.get(reg_id)

    def list_by_module(self, module: str) -> list[dict[str, Any]]:
        return [a for a in self._artifacts.values() if a["module"] == module]

    def mark_consumed(self, reg_id: str, consumer: str) -> bool:
        a = self._artifacts.get(reg_id)
        if a is None:
            return False
        a["consumed_by"].append(consumer)
        return True

    def list_all(self) -> list[dict[str, Any]]:
        return list(self._artifacts.values())

    def by_type(self, artifact_type: str) -> list[dict[str, Any]]:
        return [a for a in self._artifacts.values() if a["artifact_type"] == artifact_type]

    def artifact_summary(self) -> dict[str, Any]:
        by_type: dict[str, int] = {}
        by_module: dict[str, int] = {}
        for a in self._artifacts.values():
            by_type[a["artifact_type"]] = by_type.get(a["artifact_type"], 0) + 1
            by_module[a["module"]] = by_module.get(a["module"], 0) + 1
        return {"total": len(self._artifacts), "by_type": by_type, "by_module": by_module}


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self.channel = DataChannel()
        self.recovery = FailureRecovery()
        self.snapshot = SnapshotEngine()
        self.artifacts = ArtifactRegistry()

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def insert(self, module: str, entry: dict[str, Any]) -> None:
        self.rows(module).append(entry)

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        channel_stats = self.channel.topic_stats()
        recovery = self.recovery.recovery_summary()
        snapshot_info = self.snapshot.snapshot_summary()
        artifact_info = self.artifacts.artifact_summary()
        return {
            "cards": cards,
            "modules": modules,
            "data_channels": {"topics": len(channel_stats), "stats": channel_stats},
            "recovery": recovery,
            "snapshots": snapshot_info,
            "artifacts": artifact_info,
        }


store = Store()