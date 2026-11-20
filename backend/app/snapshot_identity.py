"""快照归属身份：模型身份指纹与归属校验异常。

名称相近不等于同一张表：``dws_sales`` 与 ``dws_sales_extra`` 共享前缀，
却是两个独立模型。快照归属必须同时凭以下三份证明确认，禁止按子串/前缀
互相挂接或回滚：

1. ``model_id``：模型身份，随表名稳定派生，改名时沿用旧身份；
2. ``model_version``：模型版本；
3. ``field_fingerprint``：字段结构指纹，字段集合不同即视为不同结构。
"""
from __future__ import annotations

import hashlib
from typing import Iterable


class SnapshotOwnershipError(Exception):
    """快照归属校验失败基类，携带全部拒绝原因以便审计。"""

    def __init__(self, message: str, *, reasons: list[str] | None = None) -> None:
        super().__init__(message)
        self.reasons = reasons or [message]


class IdentityMismatchError(SnapshotOwnershipError):
    """模型身份不一致：快照实际属于另一张表（含同名前缀的不同模型）。"""


class SchemaMismatchError(IdentityMismatchError):
    """字段结构指纹不一致：快照版本与目标模型的字段结构不兼容。"""


class ConcurrentRestoreError(SnapshotOwnershipError):
    """同一模型身份已有恢复任务持锁，并发恢复被拒绝。"""


def normalize_name(name: str | None) -> str:
    """规范化表名：去首尾空白；None 按空串处理。"""
    return str(name or "").strip()


def model_id_for(name: str) -> str:
    """由规范表名派生稳定模型身份ID，同一名称永远得到同一ID。"""
    return hashlib.sha1(normalize_name(name).encode("utf-8")).hexdigest()[:16]


def field_fingerprint(fields: Iterable[str] | None) -> str:
    """字段结构指纹：字段集合排序后哈希，字段顺序不影响结果。"""
    normalized = sorted({str(f).strip() for f in (fields or []) if str(f).strip()})
    return hashlib.sha256("\x1f".join(normalized).encode("utf-8")).hexdigest()[:16]


def identity_fingerprint(*, model_id: str, model_version: str, field_fp: str) -> str:
    """综合身份指纹：模型身份 + 版本 + 字段结构，作为归属证明摘要。"""
    raw = f"{model_id}|{model_version}|{field_fp}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def is_prefix_collision(name_a: str, name_b: str) -> bool:
    """两个名称不同但一个是另一个的严格前缀（同名前缀冲突的典型形态）。"""
    a, b = normalize_name(name_a), normalize_name(name_b)
    if not a or not b or a == b:
        return False
    return a.startswith(b) or b.startswith(a)
