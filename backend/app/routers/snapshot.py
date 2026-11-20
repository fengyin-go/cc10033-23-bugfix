"""快照计划归属治理路由：模型身份登记、改名、快照归属核对、错误挂接修复与回滚。

所有写操作都以快照固化的归属证明（模型身份+版本+字段结构）为准，
名称相近但身份不同的模型在入口即被拒绝，不写入任何错误分区。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.schemas import (
    RenamePayload,
    RepairAttachmentPayload,
    RestorePayload,
    TargetIdentityPayload,
)
from app.services import service_for
from app.snapshot_identity import SnapshotOwnershipError

router = APIRouter(prefix="/api/snapshot-plans", tags=["快照归属治理"])


def _service():
    service = service_for("snapshot_plan")
    if service is None:  # pragma: no cover - 注册表固定包含该服务
        raise HTTPException(status_code=404, detail="快照计划服务未注册")
    return service


def _ownership_failure(exc: SnapshotOwnershipError) -> HTTPException:
    return HTTPException(status_code=409, detail={"message": str(exc), "reasons": exc.reasons})


@router.post("/targets/identity")
def register_target_identity(payload: TargetIdentityPayload) -> dict:
    """登记目标模型身份及字段结构，作为后续快照归属证明的基准。"""
    try:
        return {"ok": True, "identity": _service().register_target_identity(
            payload.target_model,
            model_version=payload.model_version,
            fields=payload.fields,
        )}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/targets/rename")
def register_target_rename(payload: RenamePayload) -> dict:
    """登记真正改名：新旧名称共享同一模型身份，旧快照仍挂在原名称上。"""
    try:
        return {"ok": True, "identity": _service().register_target_rename(
            payload.old_name,
            payload.new_name,
            model_version=payload.model_version,
            fields=payload.fields,
        )}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/targets/{target_model}/snapshots")
def list_target_snapshots(
    target_model: str,
    model_version: str | None = Query(default=None),
) -> dict:
    """从快照计划列表挑选指定目标的恢复版本，名称相近的其他表不返回。"""
    return _service().list_target_snapshots(target_model, model_version=model_version)


@router.get("/targets/{target_model}/partitions")
def list_available_partitions(
    target_model: str,
    model_version: str | None = Query(default=None),
) -> dict:
    """回滚入口核对归属证明后，返回该目标当前可用分区。"""
    return _service().list_available_partitions(target_model, model_version=model_version)


@router.get("/targets/{target_model}/misattachments")
def find_misattachments(target_model: str) -> dict:
    """按原目标名称找出错误挂接的快照，精确匹配，不波及前缀相似表。"""
    return _service().find_misattachments(target_model)


@router.post("/restores")
def restore_target(payload: RestorePayload) -> dict:
    """回滚任务：核对同一份归属证明后恢复指定分区，并发恢复按身份加锁拒绝。"""
    try:
        record = _service().restore_target(
            payload.target_model,
            payload.snap_id,
            partitions=payload.partitions,
            model_version=payload.model_version,
            fields=payload.fields,
            lock_token=payload.lock_token,
        )
    except SnapshotOwnershipError as exc:
        raise _ownership_failure(exc) from exc
    return {"ok": True, "message": "恢复完成，仅写入归属证明一致的分区", "restore": record}


@router.post("/repairs")
def repair_attachment(payload: RepairAttachmentPayload) -> dict:
    """修复错误挂接：仅重写指定快照归属，不覆盖另一张表的正确历史。"""
    try:
        record = _service().repair_target_attachment(
            payload.snap_id,
            payload.expected_target,
            model_version=payload.model_version,
            fields=payload.fields,
            reason=payload.reason,
        )
    except SnapshotOwnershipError as exc:
        raise _ownership_failure(exc) from exc
    return {"ok": True, "message": "快照归属已修复", "repair": record}
