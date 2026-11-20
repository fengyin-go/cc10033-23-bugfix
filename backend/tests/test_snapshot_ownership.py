"""快照归属治理测试。

覆盖场景：
- 名称相近的独立表（dws_sales / dws_sales_extra）快照不得共享归属
- 错误挂接可按原目标找到，修复只改指定快照、不覆盖另一张表的正确历史
- 回滚请求按目标模型身份/版本/字段结构核对恢复范围
- 并发恢复按模型身份加锁，拒绝覆盖同名前缀表分区
- 真正改名沿用同一身份，旧快照仍挂原名称，两个名称不被混作一张表
- 快照计划登记只做精确/通配匹配，子串前缀事件被拒绝挂接
"""
from __future__ import annotations

import pytest

from app.snapshot_identity import (
    ConcurrentRestoreError,
    IdentityMismatchError,
    SchemaMismatchError,
    field_fingerprint,
    is_prefix_collision,
    model_id_for,
    normalize_name,
)
from app.services.snapshot_plan import SnapshotPlanService, plan_matches_target
from app.store import Store

SALES = "dws_sales"
SALES_EXTRA = "dws_sales_extra"
SALES_FIELDS = ["sale_date", "sales_amt", "order_cnt"]
EXTRA_FIELDS = ["sale_date", "extra_amt", "channel_code", "order_cnt"]


@pytest.fixture
def store() -> Store:
    return Store()


@pytest.fixture(autouse=True)
def _patch_service_store(store: Store, monkeypatch: pytest.MonkeyPatch) -> Store:
    """快照计划服务在模块级持有 store 单例，测试中统一替换为隔离实例。"""
    import app.services.snapshot_plan as snapshot_plan_module

    monkeypatch.setattr(snapshot_plan_module, "store", store)
    return store


@pytest.fixture
def service(store: Store) -> SnapshotPlanService:
    return SnapshotPlanService()


def _register_pair(store: Store) -> dict:
    """登记两张同名前缀的独立表。"""
    sales = store.snapshot.register_model(SALES, model_version="v1.0", fields=SALES_FIELDS)
    extra = store.snapshot.register_model(SALES_EXTRA, model_version="v1.0", fields=EXTRA_FIELDS)
    return {"sales": sales, "extra": extra}


def _make_snapshot(store: Store, target: str, model_id: str, version: str,
                   partitions: list[str], *, entry_id: int = 1,
                   fields: list[str] | None = None) -> str:
    if fields is None:
        fields = EXTRA_FIELDS if target == SALES_EXTRA else SALES_FIELDS
    return store.snapshot.create_version_snapshot(
        "snapshot_plan", entry_id,
        {"target_model": target, "partitions": partitions},
        version,
        target_model=target,
        model_id=model_id,
        model_version=version,
        fields=fields,
    )


# ======================================================================
# 1. 身份指纹：名称相近不等于同一张表
# ======================================================================


class TestIdentityFingerprint:
    def test_prefixed_names_have_distinct_model_ids(self) -> None:
        assert is_prefix_collision(SALES, SALES_EXTRA)
        assert model_id_for(SALES) != model_id_for(SALES_EXTRA)
        # 同一名称稳定派生，改名前后可凭登记复用同一身份
        assert model_id_for(SALES) == model_id_for(normalize_name(f"  {SALES} "))

    def test_field_fingerprint_distinguishes_structure(self) -> None:
        assert field_fingerprint(SALES_FIELDS) != field_fingerprint(EXTRA_FIELDS)
        # 字段顺序不影响指纹
        assert field_fingerprint(SALES_FIELDS) == field_fingerprint(list(reversed(SALES_FIELDS)))


# ======================================================================
# 2. 两张独立表分别登记快照，归属不得共享
# ======================================================================


class TestSeparateOwnership:
    def test_snapshots_registered_separately_keep_ownership(self, store: Store) -> None:
        ids = _register_pair(store)
        sales_snap = _make_snapshot(
            store, SALES, ids["sales"]["model_id"], "v1.0", ["ds=20261101"])
        extra_snap = _make_snapshot(
            store, SALES_EXTRA, ids["extra"]["model_id"], "v1.0", ["ds=20261102"])

        sales_owner = store.snapshot.get_snapshot(sales_snap)["ownership"]
        extra_owner = store.snapshot.get_snapshot(extra_snap)["ownership"]
        assert sales_owner["model_id"] == ids["sales"]["model_id"]
        assert extra_owner["model_id"] == ids["extra"]["model_id"]
        assert sales_owner["model_id"] != extra_owner["model_id"]
        assert sales_owner["identity_fingerprint"] != extra_owner["identity_fingerprint"]

    def test_list_by_target_is_exact_not_prefix(self, store: Store) -> None:
        ids = _register_pair(store)
        _make_snapshot(store, SALES, ids["sales"]["model_id"], "v1.0", ["ds=20261101"])
        _make_snapshot(store, SALES_EXTRA, ids["extra"]["model_id"], "v1.0", ["ds=20261102"])

        sales_list = store.snapshot.list_snapshots_by_target(SALES)
        extra_list = store.snapshot.list_snapshots_by_target(SALES_EXTRA)
        assert {s["snap_id"] for s in sales_list} == {
            s["snap_id"] for s in store.snapshot.list_snapshots_by_target(f" {SALES} ")}
        assert len(sales_list) == 1
        assert len(extra_list) == 1
        assert sales_list[0]["ownership"]["target_model"] == SALES
        assert extra_list[0]["ownership"]["target_model"] == SALES_EXTRA

    def test_service_listing_filters_similar_named_models(self, service: SnapshotPlanService) -> None:
        result_sales = service.list_target_snapshots(SALES)
        result_extra = service.list_target_snapshots(SALES_EXTRA)
        assert result_sales["model_id"] != result_extra["model_id"]
        assert result_sales["target_model"] == SALES
        assert result_extra["target_model"] == SALES_EXTRA


# ======================================================================
# 3. 错误挂接发现与修复
# ======================================================================


class TestMisattachmentRepair:
    def test_find_misattached_by_original_target(self, store: Store) -> None:
        ids = _register_pair(store)
        # 同一计划条目（entry_id=7）下混入两张表的快照，其中 extra 是错误挂接
        _make_snapshot(store, SALES, ids["sales"]["model_id"], "v1.0",
                       ["ds=20261101"], entry_id=7)
        wrong = _make_snapshot(store, SALES_EXTRA, ids["extra"]["model_id"], "v1.0",
                               ["ds=20261102"], entry_id=7)

        findings = store.snapshot.find_misattached(SALES)
        assert len(findings) == 1
        assert findings[0]["snap_id"] == wrong
        assert findings[0]["expected_target"] == SALES
        assert findings[0]["actual_target"] == SALES_EXTRA
        assert findings[0]["prefix_collision"] is True

    def test_repair_only_rewrites_named_snapshot(self, store: Store) -> None:
        ids = _register_pair(store)
        good = _make_snapshot(store, SALES, ids["sales"]["model_id"], "v1.0",
                              ["ds=20261101"], entry_id=7)
        # 归属 sales 的快照被错误登记成 extra 的名称（典型错误挂接）
        wrong = store.snapshot.create_version_snapshot(
            "snapshot_plan", 7,
            {"target_model": SALES_EXTRA, "partitions": ["ds=20261102"]},
            "v1.0",
            target_model=SALES_EXTRA,
            model_id=ids["sales"]["model_id"],  # 真实身份仍是 sales
            model_version="v1.0",
            fields=SALES_FIELDS,
        )

        # 按原目标 sales 能找到这条错误挂接
        findings = store.snapshot.find_misattached(SALES)
        assert [f["snap_id"] for f in findings] == [wrong]

        # 真实身份属于 sales 的快照不能修成 extra
        with pytest.raises(IdentityMismatchError) as exc:
            store.snapshot.repair_attachment(wrong, expected_target=SALES_EXTRA)
        assert "ownership_proof_mismatch" in exc.value.reasons
        # 正确历史未被改动
        assert store.snapshot.get_snapshot(good)["ownership"]["target_model"] == SALES

        # 按真实目标修复：名称改回 sales，身份保持不变
        record = store.snapshot.repair_attachment(
            wrong, expected_target=SALES, fields=SALES_FIELDS)
        assert record["before"]["target_model"] == SALES_EXTRA
        assert record["after"]["target_model"] == SALES
        repaired = store.snapshot.get_snapshot(wrong)
        assert repaired["ownership"]["target_model"] == SALES
        assert repaired["ownership"]["model_id"] == ids["sales"]["model_id"]
        assert repaired["repair_history"][0]["reason"] == "manual_repair"
        # sales 的另一条正确历史保持不变，extra 名下没有被写入任何快照
        assert store.snapshot.get_snapshot(good)["ownership"]["target_model"] == SALES
        assert store.snapshot.list_snapshots_by_target(SALES_EXTRA) == []
        assert len(store.snapshot.repair_history()) == 1
        # 修复后错误挂接清零
        assert store.snapshot.find_misattached(SALES) == []

    def test_repair_unknown_target_refused(self, store: Store) -> None:
        ids = _register_pair(store)
        snap = _make_snapshot(store, SALES, ids["sales"]["model_id"], "v1.0", ["ds=20261101"])
        with pytest.raises(IdentityMismatchError) as exc:
            store.snapshot.repair_attachment(snap, expected_target="dws_never_registered")
        assert "target_model_unregistered" in exc.value.reasons


# ======================================================================
# 4. 回滚入口：身份/版本/字段结构核对恢复范围
# ======================================================================


class TestVerifiedRestore:
    def test_restore_accepts_matching_ownership(self, store: Store) -> None:
        ids = _register_pair(store)
        snap = _make_snapshot(
            store, SALES, ids["sales"]["model_id"], "v1.0",
            ["ds=20261101", "ds=20261102"])
        record = store.snapshot.restore_target_snapshot(
            target_model=SALES, snap_id=snap, partitions=["ds=20261101"],
            model_id=ids["sales"]["model_id"], model_version="v1.0", fields=SALES_FIELDS)
        assert record["partitions"] == ["ds=20261101"]
        assert record["identity_fingerprint"] == \
            store.snapshot.get_snapshot(snap)["ownership"]["identity_fingerprint"]
        # 恢复记录可证明快照真实归属
        history = store.snapshot.restore_history(SALES)
        assert len(history) == 1
        assert history[0]["model_id"] == ids["sales"]["model_id"]

    def test_restore_refuses_similar_named_model_snapshot(self, store: Store) -> None:
        ids = _register_pair(store)
        extra_snap = _make_snapshot(
            store, SALES_EXTRA, ids["extra"]["model_id"], "v1.0", ["ds=20261102"])
        before = len(store.snapshot.restored_partitions())
        with pytest.raises(IdentityMismatchError) as exc:
            store.snapshot.restore_target_snapshot(
                target_model=SALES, snap_id=extra_snap,
                model_id=ids["sales"]["model_id"])
        assert any("模型身份不符" in r for r in exc.value.reasons)
        # 没有任何来自错误快照的分区被写入
        assert len(store.snapshot.restored_partitions()) == before
        assert store.snapshot.restore_history(SALES) == []

    def test_restore_refuses_wrong_field_structure(self, store: Store) -> None:
        ids = _register_pair(store)
        snap = _make_snapshot(store, SALES, ids["sales"]["model_id"], "v1.0", ["ds=20261101"])
        with pytest.raises(SchemaMismatchError):
            store.snapshot.restore_target_snapshot(
                target_model=SALES, snap_id=snap,
                model_id=ids["sales"]["model_id"], fields=EXTRA_FIELDS)
        assert store.snapshot.restore_history(SALES) == []

    def test_restore_refuses_wrong_version(self, store: Store) -> None:
        ids = _register_pair(store)
        snap = _make_snapshot(store, SALES, ids["sales"]["model_id"], "v1.0", ["ds=20261101"])
        with pytest.raises(IdentityMismatchError) as exc:
            store.snapshot.restore_target_snapshot(
                target_model=SALES, snap_id=snap, model_version="v9.9")
        assert any("模型版本不符" in r for r in exc.value.reasons)

    def test_restore_rejects_partition_out_of_scope(self, store: Store) -> None:
        ids = _register_pair(store)
        snap = _make_snapshot(store, SALES, ids["sales"]["model_id"], "v1.0", ["ds=20261101"])
        with pytest.raises(IdentityMismatchError) as exc:
            store.snapshot.restore_target_snapshot(
                target_model=SALES, snap_id=snap, partitions=["ds=19990101"],
                model_id=ids["sales"]["model_id"])
        assert "partition_out_of_scope" in exc.value.reasons
        assert store.snapshot.restored_partitions(SALES) == []

    def test_available_partitions_only_from_owned_snapshots(
            self, service: SnapshotPlanService, store: Store) -> None:
        ids = _register_pair(store)
        _make_snapshot(store, SALES, ids["sales"]["model_id"], "v1.0", ["ds=20261101"])
        _make_snapshot(store, SALES_EXTRA, ids["extra"]["model_id"], "v1.0", ["ds=20261102"])
        result = service.list_available_partitions(SALES)
        assert [p["partition"] for p in result["partitions"]] == ["ds=20261101"]
        assert all(p["model_id"] == ids["sales"]["model_id"] for p in result["partitions"])


# ======================================================================
# 5. 并发恢复锁
# ======================================================================


class TestConcurrentRestoreLock:
    def test_concurrent_restore_on_same_model_blocked(self, store: Store) -> None:
        ids = _register_pair(store)
        snap = _make_snapshot(store, SALES, ids["sales"]["model_id"], "v1.0", ["ds=20261101"])
        token = store.snapshot.acquire_restore_lock(ids["sales"]["model_id"])
        with pytest.raises(ConcurrentRestoreError):
            store.snapshot.acquire_restore_lock(ids["sales"]["model_id"])
        # 持锁期间通过回滚入口的并发恢复同样被拒绝
        with pytest.raises(ConcurrentRestoreError):
            store.snapshot.restore_target_snapshot(
                target_model=SALES, snap_id=snap,
                model_id=ids["sales"]["model_id"], lock_token="other-task")
        # 持锁任务用同一令牌可以继续，释放后其他任务可进入
        record = store.snapshot.restore_target_snapshot(
            target_model=SALES, snap_id=snap,
            model_id=ids["sales"]["model_id"], lock_token=token)
        assert record["lock_token"] == token
        assert store.snapshot.release_restore_lock(ids["sales"]["model_id"], token)
        new_token = store.snapshot.acquire_restore_lock(ids["sales"]["model_id"])
        assert new_token != token

    def test_lock_is_per_model_identity(self, store: Store) -> None:
        ids = _register_pair(store)
        store.snapshot.acquire_restore_lock(ids["sales"]["model_id"])
        # 另一张同名前缀表是不同身份，并发恢复互不阻塞
        token = store.snapshot.acquire_restore_lock(ids["extra"]["model_id"])
        assert token


# ======================================================================
# 6. 真正改名：同一身份、旧快照挂原名称
# ======================================================================


class TestRenameOwnership:
    def test_rename_keeps_identity_and_legacy_snapshot_name(self, store: Store) -> None:
        identity = store.snapshot.register_model(
            SALES, model_version="v1.0", fields=SALES_FIELDS)
        legacy = _make_snapshot(
            store, SALES, identity["model_id"], "v1.0", ["ds=20261101"])

        renamed = store.snapshot.register_rename(
            SALES, "dws_sales_renamed", model_version="v1.1", fields=SALES_FIELDS)
        assert renamed["model_id"] == identity["model_id"]
        assert renamed["previous_name"] == SALES

        # 旧快照仍挂在原名称上
        assert store.snapshot.get_snapshot(legacy)["ownership"]["target_model"] == SALES
        # 两个名称都解析到同一模型身份，不会被当作两张表
        assert store.snapshot.resolve_identity(SALES)["model_id"] == identity["model_id"]
        assert store.snapshot.resolve_identity("dws_sales_renamed")["model_id"] == identity["model_id"]
        # 按身份可以同时取到旧名称下的历史快照
        snaps = store.snapshot.list_snapshots_by_target(
            "dws_sales_renamed", model_id=identity["model_id"])
        assert {s["snap_id"] for s in snaps} == {legacy}

    def test_rename_cannot_merge_two_distinct_models(self, store: Store) -> None:
        _register_pair(store)
        with pytest.raises(ValueError):
            store.snapshot.register_rename(SALES, SALES_EXTRA)

    def test_reregistration_without_fields_keeps_fingerprint(self, store: Store) -> None:
        """重复登记但不带字段结构时，不得用空结构覆盖已固化的字段指纹。"""
        first = store.snapshot.register_model(SALES, model_version="v1.0", fields=SALES_FIELDS)
        second = store.snapshot.register_model(SALES, model_version="v1.0")
        assert second["field_fingerprint"] == first["field_fingerprint"]
        # 启用计划产生的初始快照也必须继承真实字段结构
        svc = SnapshotPlanService()
        entry, _ = svc.create_entry({"策略名称": "p", "目标表": SALES, "快照周期": "每日"})
        svc.run_action(entry["id"], "启用策略")
        snap = store.snapshot.list_snapshots_by_target(SALES)[0]
        assert snap["ownership"]["field_fingerprint"] == first["field_fingerprint"]


# ======================================================================
# 7. 快照计划登记：精确/通配匹配，拒绝子串前缀挂接
# ======================================================================


class TestPlanMatching:
    def test_plan_match_rules(self) -> None:
        assert plan_matches_target(SALES, SALES)
        assert plan_matches_target("dws_*", SALES)
        assert plan_matches_target("dws_*", SALES_EXTRA)
        # 子串/前缀相似但计划是具体表名：不匹配
        assert not plan_matches_target(SALES, SALES_EXTRA)
        assert not plan_matches_target(SALES_EXTRA, SALES)
        assert not plan_matches_target(SALES, "dws_sales_backup")

    def test_create_entry_rejects_prefix_event_attachment(
            self, service: SnapshotPlanService, store: Store) -> None:
        store.channel.publish("deploy.published", {
            "task_name": "deploy-extra", "target_model": SALES_EXTRA,
            "environment": "生产", "source_version": "v1.0",
            "entry_id": 9001, "partitions": ["ds=20261102"],
        })
        entry, missing = service.create_entry({
            "策略名称": "sales快照计划", "目标表": SALES, "快照周期": "每日",
        })
        assert missing == []
        assert entry["目标表"] == SALES
        # 拒绝事件通过通道留痕
        rejected = store.channel.topic_snapshot("snapshot.ownership_rejected")
        assert len(rejected) == 1
        assert rejected[0]["payload"]["rejections"][0]["event_target"] == SALES_EXTRA
        # 错误事件没有产生任何挂到 sales 计划的快照
        assert store.snapshot.list_snapshots_by_target(SALES) == []
        assert store.snapshot.list_snapshots_by_target(SALES_EXTRA) == []

    def test_create_entry_attaches_exact_event_with_ownership(
            self, service: SnapshotPlanService, store: Store) -> None:
        store.channel.publish("deploy.published", {
            "task_name": "deploy-sales", "target_model": SALES,
            "environment": "生产", "source_version": "v1.0",
            "entry_id": 9002, "partitions": ["ds=20261101"],
        })
        entry, _ = service.create_entry({
            "策略名称": "sales快照计划", "目标表": SALES, "快照周期": "每日",
            "字段结构": ",".join(SALES_FIELDS),
        })
        snaps = store.snapshot.list_snapshots_by_target(SALES)
        assert len(snaps) == 1
        owner = snaps[0]["ownership"]
        assert owner["target_model"] == SALES
        assert owner["model_version"] == "v1.0"
        assert owner["field_fingerprint"] == field_fingerprint(SALES_FIELDS)
        # 计划条目登记了目标模型身份，与快照归属一致
        registered = store.snapshot.resolve_identity(SALES, fields=SALES_FIELDS)
        assert registered["model_id"] == owner["model_id"]
        assert registered["field_fingerprint"] == field_fingerprint(SALES_FIELDS)

    def test_wildcard_plan_binds_event_identity_not_plan_name(
            self, service: SnapshotPlanService, store: Store) -> None:
        store.channel.publish("deploy.published", {
            "task_name": "deploy-extra", "target_model": SALES_EXTRA,
            "environment": "预发", "source_version": "v2.0",
            "entry_id": 9003, "partitions": ["ds=20261103"],
        })
        service.create_entry({
            "策略名称": "dws通配计划", "目标表": "dws_*", "快照周期": "每日",
        })
        snaps = store.snapshot.list_snapshots_by_target(SALES_EXTRA)
        assert len(snaps) == 1
        assert snaps[0]["ownership"]["target_model"] == SALES_EXTRA
        assert snaps[0]["ownership"]["model_id"] == model_id_for(SALES_EXTRA)

    def test_enable_action_creates_owned_initial_snapshot(
            self, service: SnapshotPlanService, store: Store) -> None:
        entry, _ = service.create_entry({
            "策略名称": "sales启用计划", "目标表": SALES, "快照周期": "每日",
        })
        service.run_action(entry["id"], "启用策略")
        snaps = store.snapshot.list_snapshots_by_target(SALES)
        assert len(snaps) == 1
        assert snaps[0]["ownership"]["model_id"] == model_id_for(SALES)
        # 检查点固化模型身份
        checkpoints = [
            cp for cp in store.recovery._checkpoints.values()
            if cp["task_id"] == f"snap_{entry['id']}"
        ]
        assert checkpoints[0]["state"]["model_id"] == model_id_for(SALES)


# ======================================================================
# 8. 端到端：回滚任务从计划中挑选版本，错误快照一个分区都写不进
# ======================================================================


class TestEndToEndRollback:
    def test_full_flow_two_similar_tables(self, service: SnapshotPlanService,
                                          store: Store) -> None:
        service.register_target_identity(SALES, fields=SALES_FIELDS)
        service.register_target_identity(SALES_EXTRA, fields=EXTRA_FIELDS)

        sales_id = model_id_for(SALES)
        extra_id = model_id_for(SALES_EXTRA)
        sales_snap = _make_snapshot(
            store, SALES, sales_id, "v1.0", ["ds=20261101", "ds=20261102"])
        _make_snapshot(store, SALES_EXTRA, extra_id, "v1.0", ["ds=20261103"])

        # 回滚入口只能看到本目标的版本与分区
        listing = service.list_target_snapshots(SALES)
        assert [item["snap_id"] for item in listing["items"]] == [sales_snap]
        partitions = service.list_available_partitions(SALES)
        assert {p["partition"] for p in partitions["partitions"]} == {
            "ds=20261101", "ds=20261102"}

        # 拿 extra 的快照请求恢复 sales：拒绝且零分区写入
        extra_snap = service.list_target_snapshots(SALES_EXTRA)["items"][0]["snap_id"]
        with pytest.raises(IdentityMismatchError):
            service.restore_target(SALES, extra_snap)
        assert store.snapshot.restored_partitions(SALES) == []

        # 正确快照按范围恢复成功，记录携带归属证明
        record = service.restore_target(
            SALES, sales_snap, partitions=["ds=20261101"], fields=SALES_FIELDS)
        assert record["target_model"] == SALES
        assert record["model_id"] == sales_id
        restored = store.snapshot.restored_partitions(SALES)
        assert [r["partition"] for r in restored] == ["ds=20261101"]
        assert all(r["model_id"] == sales_id for r in restored)
        # extra 的分区台账完全不受影响
        assert store.snapshot.restored_partitions(SALES_EXTRA) == []
