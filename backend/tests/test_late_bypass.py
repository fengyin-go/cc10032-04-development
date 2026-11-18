"""迟到旁路链路测试：水位记录 → 迟到事件 → 旁路版本 → 补数作业/回执。

覆盖：
  - 读取水位记录并按允许延迟识别迟到事件；
  - 同一事件重复到达只记一次；
  - 同一请求/同一水位口径只落一次记录（幂等）；
  - 每个迟到事件只能进入一个旁路版本，重复到达不新增第二个补数范围；
  - 补数任务范围覆盖该版本的水位区间，回执保留被覆盖的事件范围；
  - 两种口径同时提交只落一组记录，并发重试只接收整组一次；
  - 冻结/生成作业/写回执的事务失败整组回退；
  - 故障重开沿用同一口径，重复重试只记一次；
  - 其他页面生成的待办随状态推进关闭。
"""
from __future__ import annotations

import threading

import pytest

from app.services import late_bypass as lb
from app.store import store


@pytest.fixture(autouse=True)
def _clean_store():
    store.reset()
    yield
    store.reset()


def _freeze(watermark_id: int, scope: str = "current", request_id: str = "req"):
    return lb.freeze_version(watermark_id, scope, request_id=request_id)


# ---------------------------------------------------------------------------
# 水位记录读取与迟到识别
# ---------------------------------------------------------------------------

class TestWatermarkLateDetection:
    def test_reads_watermark_record(self):
        detail = lb.late_distribution(4)
        assert detail["watermark"]["数据源"] == "exposure_stream"
        assert detail["watermark"]["当前水位"] == 1788314400.0
        assert detail["rule_hit"]["condition_range"]["cutoff"] == 1788314355

    def test_late_events_respect_allowed_lateness(self):
        analysis = lb.analyse_late(store.find("watermark_track", 4), "current")
        cutoff = 1788314400 - 45
        assert analysis["late_count"] == 4
        assert all(event["event_time"] < cutoff for event in analysis["late_events"])
        assert all(event["event_time"] >= cutoff for event in analysis["on_time_events"])

    def test_interval_bounds_are_previous_watermark_exclusive(self):
        analysis = lb.analyse_late(store.find("watermark_track", 4), "current")
        assert analysis["interval"]["start"] == 1788310800
        assert analysis["interval"]["end"] == 1788314400
        assert all(
            analysis["interval"]["start"] < event["event_time"] <= analysis["interval"]["end"]
            for event in analysis["events"]
        )

    def test_lookback_scope_includes_previous_window(self):
        record = store.find("watermark_track", 1)
        current = lb.analyse_late(record, "current")
        lookback = lb.analyse_late(record, "lookback")
        assert lookback["interval"]["start"] < current["interval"]["start"]
        assert lookback["late_count"] > current["late_count"]

    def test_duplicate_arrivals_counted_once(self):
        analysis = lb.analyse_late(store.find("watermark_track", 1), "current")
        ids = [event["event_id"] for event in analysis["late_events"]]
        assert ids.count("evt_order_0003") == 1
        assert analysis["duplicate_count"] == 1

    def test_threshold_hit_uses_policy_ratio(self):
        detail = lb.late_distribution(2)
        assert detail["rule_hit"]["policy"] == "支付迟到旁路策略"
        assert detail["rule_hit"]["threshold"] == pytest.approx(0.15)
        assert detail["rule_hit"]["matched"] is True

    def test_threshold_miss_keeps_rule_hit_false(self):
        detail = lb.late_distribution(3)
        assert detail["rule_hit"]["policy"] == "库存迟到旁路策略"
        assert detail["rule_hit"]["matched"] is False

    def test_distribution_buckets_use_allowed_lateness_units(self):
        detail = lb.late_distribution(4)
        counts = {bucket["bucket"]: bucket["count"] for bucket in detail["distribution"]}
        assert sum(counts.values()) == detail["scopes"]["current"]["late_count"]
        assert counts["0-1x"] >= 0


# ---------------------------------------------------------------------------
# 冻结旁路版本：幂等与事件归属
# ---------------------------------------------------------------------------

class TestFreezeVersion:
    def test_freeze_creates_version_and_event_links(self):
        result = _freeze(4, "current", "req-a")
        assert result["created"] is True
        version = result["version"]
        assert version["口径"] == "current"
        assert version["水位区间"] == "1788310800-1788314400"
        assert version["补数范围"] == version["水位区间"]
        assert version["迟到事件数"] == 4
        links = lb._events_of_version(version["id"])
        assert len(links) == 4

    def test_same_request_is_idempotent(self):
        first = _freeze(4, "current", "req-same")
        second = _freeze(4, "current", "req-same")
        assert second["created"] is False
        assert second["idempotent"] is True
        assert second["version"]["id"] == first["version"]["id"]
        assert len(store.rows("late_bypass")) == 1

    def test_same_interval_and_scope_is_idempotent_without_request(self):
        first = lb.freeze_version(4, "current")
        second = lb.freeze_version(4, "current")
        assert first["created"] is True
        assert second["created"] is False
        assert second["version"]["id"] == first["version"]["id"]
        assert len(store.rows("late_bypass")) == 1

    def test_two_scopes_create_two_versions(self):
        current = _freeze(4, "current", "req-c")
        lookback = _freeze(4, "lookback", "req-l")
        assert current["version"]["id"] != lookback["version"]["id"]
        assert lookback["version"]["区间起始"] < current["version"]["区间起始"]

    def test_event_belongs_to_only_one_version(self):
        current = _freeze(4, "current", "req-c")
        lookback = _freeze(4, "lookback", "req-l")
        current_ids = {row["event_key"] for row in lb._events_of_version(current["version"]["id"])}
        lookback_ids = {row["event_key"] for row in lb._events_of_version(lookback["version"]["id"])}
        assert current_ids.isdisjoint(lookback_ids)
        assert current_ids | lookback_ids == {
            row["event_key"] for row in store.rows("late_bypass_event")
        }

    def test_repeat_freeze_does_not_add_second_range(self):
        _freeze(4, "current", "req-1")
        before_jobs = len(store.rows("backfill_job"))
        again = lb.freeze_version(4, "current")
        assert again["created"] is False
        assert len(store.rows("late_bypass")) == 1
        assert len(store.rows("backfill_job")) == before_jobs

    def test_repeat_arrival_after_bypass_is_skipped(self):
        first = _freeze(4, "current", "req-1")
        # 同一事件再到达一次：库里多一条重复事件，但不新增版本也不新增补数范围。
        store.rows("watermark_event").append({
            "id": 999,
            "event_id": "evt_exp_0002",
            "数据源": "exposure_stream",
            "watermark_id": 4,
            "event_time": 1788311200.0,
            "arrival_time": 1788315000.0,
        })
        frozen = lb.freeze_version(4, "lookback", request_id="req-2")
        second = lb.freeze_version(4, "current")
        assert second["created"] is False
        # 已旁路事件不会进入 lookback 版本
        lookback_links = lb._events_of_version(frozen["version"]["id"])
        assert all(row["event_id"] != "evt_exp_0002" for row in lookback_links)
        assert len(lb._events_of_version(first["version"]["id"])) == 4

    def test_current_result_written_for_each_scope(self):
        _freeze(4, "current", "req-c")
        _freeze(4, "lookback", "req-l")
        rows = lb.current_result_for("exposure_stream")
        assert {row["口径"] for row in rows} == {"current", "lookback"}
        assert all(row["可执行"] is False for row in rows)

    def test_freeze_transaction_failure_rolls_back(self):
        with pytest.raises(lb.LateBypassError) as error:
            lb.freeze_version(4, "current", request_id="bad", inject_failure=True)
        assert error.value.code == "transaction_failed"
        assert store.rows("late_bypass") == []
        assert store.rows("late_bypass_event") == []
        assert store.rows("late_bypass_current") == []
        assert store.rows("late_todo") == []

    def test_retry_after_failure_keeps_same_scope(self):
        with pytest.raises(lb.LateBypassError):
            lb.freeze_version(4, "current", request_id="retry-1", inject_failure=True)
        retried = lb.freeze_version(4, "current", request_id="retry-1")
        assert retried["created"] is True
        assert retried["version"]["口径"] == "current"
        assert retried["version"]["补数范围"] == "1788310800-1788314400"
        assert len(store.rows("late_bypass")) == 1

    def test_version_detail_reports_rule_hit(self):
        frozen = _freeze(2, "current", "req-c")
        detail = lb.version_detail(frozen["version"]["id"])
        assert detail["rule_hit"]["matched"] is True
        assert detail["rule_hit"]["policy"] == "支付迟到旁路策略"

    def test_freeze_unknown_watermark(self):
        with pytest.raises(lb.LateBypassError) as error:
            lb.freeze_version(9999, "current")
        assert error.value.code == "watermark_not_found"

    def test_freeze_invalid_scope(self):
        with pytest.raises(lb.LateBypassError) as error:
            lb.freeze_version(4, "future")
        assert error.value.code == "invalid_scope"


# ---------------------------------------------------------------------------
# 整组提交：两种口径 + 并发重试
# ---------------------------------------------------------------------------

class TestGroupSubmit:
    def test_group_submit_creates_two_versions_once(self):
        result = lb.submit_group(4, request_id="grp-1")
        assert result["created"] is True
        assert {row["口径"] for row in result["versions"]} == {"current", "lookback"}
        assert len(store.rows("late_submission")) == 1

    def test_group_submit_repeat_is_idempotent(self):
        lb.submit_group(4, request_id="grp-1")
        again = lb.submit_group(4, request_id="grp-1")
        assert again["idempotent"] is True
        assert len(store.rows("late_bypass")) == 2
        assert len(store.rows("late_submission")) == 1

    def test_group_submit_concurrent_retry_records_once(self):
        results: list[dict] = []
        barrier = threading.Barrier(8)

        def worker() -> None:
            barrier.wait()
            results.append(lb.submit_group(2, request_id="grp-race"))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(results) == 8
        assert all(result["accepted"] for result in results)
        assert sum(1 for result in results if result["created"]) == 1
        assert len(store.rows("late_submission")) == 1
        assert len(store.rows("late_bypass")) == 2

    def test_group_failure_rolls_back_whole_group(self):
        with pytest.raises(lb.LateBypassError):
            lb.submit_group(4, request_id="bad-group", inject_failure=True)
        assert store.rows("late_bypass") == []
        assert store.rows("late_submission") == []


# ---------------------------------------------------------------------------
# 确认范围 → 生成作业 → 回执
# ---------------------------------------------------------------------------

class TestBackfillChain:
    def test_confirm_creates_job_covering_interval(self):
        frozen = _freeze(4, "current", "req-c")
        confirmed = lb.confirm_version(frozen["version"]["id"], request_id="confirm-1")
        assert confirmed["created"] is True
        assert confirmed["range_covers_interval"] is True
        job = confirmed["job"]
        assert int(job["起始位点"]) <= 1788310800
        assert int(job["截止位点"]) >= 1788314400
        assert job["status"] == "待执行"

    def test_confirm_is_idempotent(self):
        frozen = _freeze(4, "current", "req-c")
        first = lb.confirm_version(frozen["version"]["id"], request_id="confirm-1")
        second = lb.confirm_version(frozen["version"]["id"], request_id="confirm-2")
        assert second["created"] is False
        assert second["idempotent"] is True
        assert second["job"]["id"] == first["job"]["id"]

    def test_current_result_marks_executable_range(self):
        frozen = _freeze(4, "current", "req-c")
        _freeze(4, "lookback", "req-l")
        lb.confirm_version(frozen["version"]["id"])
        rows = {row["口径"]: row for row in lb.current_result_for("exposure_stream")}
        assert rows["current"]["可执行"] is True
        assert rows["lookback"]["可执行"] is False

    def test_execute_receipt_preserves_covered_event_range(self):
        frozen = _freeze(4, "current", "req-c")
        job = lb.confirm_version(frozen["version"]["id"])["job"]
        executed = lb.execute_job(job["id"], request_id="exec-1")
        assert executed["created"] is True
        receipt = executed["receipt"]
        assert receipt["覆盖事件数"] == 4
        assert receipt["覆盖事件ID列表"] == frozen["version"]["事件ID列表"]
        assert receipt["覆盖事件范围"] == "1788314100-1788314300"
        assert receipt["水位区间"] == "1788310800-1788314400"
        assert receipt["尝试次数"] == 1

    def test_execute_repeat_only_one_receipt(self):
        frozen = _freeze(4, "current", "req-c")
        job = lb.confirm_version(frozen["version"]["id"])["job"]
        lb.execute_job(job["id"], request_id="exec-1")
        again = lb.execute_job(job["id"], request_id="exec-1")
        assert again["created"] is False
        assert again["idempotent"] is True
        assert len(store.rows("backfill_receipt")) == 1

    def test_execute_failure_rolls_back_status(self):
        frozen = _freeze(4, "current", "req-c")
        job = lb.confirm_version(frozen["version"]["id"])["job"]
        with pytest.raises(lb.LateBypassError):
            lb.execute_job(job["id"], inject_failure=True)
        assert store.find("backfill_job", job["id"])["status"] == "待执行"
        assert store.find("late_bypass", frozen["version"]["id"])["status"] == "待执行"
        assert store.rows("backfill_receipt") == []

    def test_job_list_exposes_version_and_coverage(self):
        frozen = _freeze(4, "current", "req-c")
        job = lb.confirm_version(frozen["version"]["id"])["job"]
        rows = lb.list_jobs()
        assert len(rows) == 1
        assert rows[0]["版本号"] == frozen["version"]["版本号"]
        assert rows[0]["范围覆盖"] == "覆盖"
        assert rows[0]["覆盖事件范围"] == "1788314100-1788314300"
        assert rows[0]["规则命中"] == "是"

    def test_reopen_keeps_scope_and_counts_retry_once(self):
        frozen = _freeze(4, "current", "req-c")
        job = lb.confirm_version(frozen["version"]["id"])["job"]
        lb.execute_job(job["id"], request_id="exec-1")
        lb.mark_partial_failure(frozen["version"]["id"])
        reopened = lb.reopen_version(frozen["version"]["id"], request_id="reopen-1")
        assert reopened["created"] is True
        assert reopened["version"]["口径"] == "current"
        again = lb.reopen_version(frozen["version"]["id"], request_id="reopen-1")
        assert again["created"] is False
        assert again["idempotent"] is True
        assert len(store.rows("late_bypass")) == 1
        assert store.find("late_bypass", frozen["version"]["id"])["_attempt"] == 2

    def test_void_releases_events_for_new_version(self):
        first = _freeze(4, "current", "req-1")
        lb.void_version(first["version"]["id"])
        second = _freeze(4, "current", "req-2")
        assert second["created"] is True
        assert store.find("late_bypass", first["version"]["id"])["status"] == "已作废"
        assert len(lb._events_of_version(second["version"]["id"])) == 4
        row = next(item for item in lb.audit()["rows"] if item["水位记录ID"] == 4)
        assert row["未旁路事件数"] == 0

    def test_reopen_requires_failed_or_completed(self):
        frozen = _freeze(4, "current", "req-c")
        with pytest.raises(lb.LateBypassError) as error:
            lb.reopen_version(frozen["version"]["id"], request_id="reopen-1")
        assert error.value.code == "reopen_not_allowed"


# ---------------------------------------------------------------------------
# 页面数据与待办
# ---------------------------------------------------------------------------

class TestViewsAndTodos:
    def test_watermark_overview_reports_source_relations(self):
        frozen = _freeze(4, "current", "req-c")
        job = lb.confirm_version(frozen["version"]["id"])["job"]
        lb.execute_job(job["id"])
        row = next(item for item in lb.watermark_overview() if item["id"] == 4)
        assert row["旁路版本数"] == 1
        assert row["补数作业数"] == 1
        assert row["回执数"] == 1
        assert row["来源关系"][0]["旁路版本ID"] == frozen["version"]["id"]
        assert row["来源关系"][0]["作业ID"] == job["id"]

    def test_audit_flags_uncovered_events_and_ranges(self):
        frozen = _freeze(4, "current", "req-c")
        report = lb.audit()
        row = next(item for item in report["rows"] if item["水位记录ID"] == 4)
        assert row["未旁路事件数"] == 0
        assert row["补数作业数"] == 0
        assert row["state"] == "待生成补数"

        job = lb.confirm_version(frozen["version"]["id"])["job"]
        row = next(item for item in lb.audit()["rows"] if item["水位记录ID"] == 4)
        assert row["state"] == "一致"
        assert row["补数作业"][0]["作业ID"] == job["id"]
        assert row["补数作业"][0]["覆盖"] is True

    def test_audit_without_versions_reports_pending_bypass(self):
        report = lb.audit(watermark_id=1)
        row = report["rows"][0]
        assert row["旁路版本数"] == 0
        assert row["未旁路事件数"] > 0
        assert row["state"] == "缺旁路"

    def test_audit_range_filter(self):
        report = lb.audit(start=1788310000, end=1788311000)
        assert {row["水位记录ID"] for row in report["rows"]} == {2, 4}
        assert report["summary"]["checked"] == 2

    def test_todos_created_and_closed(self):
        frozen = _freeze(4, "current", "req-c")
        open_todos = [todo for todo in lb.list_todos() if todo["status"] == "待处理"]
        assert len(open_todos) == 1
        assert open_todos[0]["module"] == "watermark_track"

        job = lb.confirm_version(frozen["version"]["id"])["job"]
        modules = {
            todo["module"] for todo in lb.list_todos() if todo["status"] == "待处理"
        }
        assert modules == {"backfill_job"}

        lb.execute_job(job["id"])
        assert [todo for todo in lb.list_todos() if todo["status"] == "待处理"] == []


# ---------------------------------------------------------------------------
# 与既有水位/补数服务的基础约束保持一致
# ---------------------------------------------------------------------------

class TestExistingContracts:
    def test_watermark_list_keeps_numeric_values(self):
        rows, total = lb.store.rows("watermark_track"), len(store.rows("watermark_track"))
        assert total == 4
        assert all(isinstance(row["当前水位"], float) for row in rows)

    def test_summary_counts(self):
        _freeze(4, "current", "req-c")
        lb.confirm_version(1)
        data = lb.summary()
        assert data["versions"] == 1
        assert data["jobs"] == 1
        assert data["scopes"]["current"] == 1
