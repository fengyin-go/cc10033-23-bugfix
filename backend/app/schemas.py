"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class TargetIdentityPayload(BaseModel):
    """登记目标模型身份/改名：名称 + 版本 + 字段结构。"""
    target_model: str
    model_version: str = "v1.0"
    fields: list[str] = Field(default_factory=list)


class RenamePayload(BaseModel):
    """真正改名登记：旧名称与新名称共享同一模型身份。"""
    old_name: str
    new_name: str
    model_version: str = "v1.0"
    fields: list[str] = Field(default_factory=list)


class RestorePayload(BaseModel):
    """回滚请求：必须核对目标模型身份及字段结构后才允许恢复分区。"""
    target_model: str
    snap_id: str
    partitions: list[str] = Field(default_factory=list)
    model_version: str | None = None
    fields: list[str] = Field(default_factory=list)
    lock_token: str | None = None


class RepairAttachmentPayload(BaseModel):
    """错误挂接修复：按原目标核对真实身份后重写指定快照归属。"""
    snap_id: str
    expected_target: str
    model_version: str | None = None
    fields: list[str] = Field(default_factory=list)
    reason: str = "manual_repair"
