"""迟到旁路领域服务：水位记录 → 迟到事件 → 旁路版本 → 补数作业/回执。

链路：
  watermark_track（读水位记录）
    → late_bypass（迟到旁路数据版本，按口径冻结）
    → late_bypass_event（事件归属，保证一个事件只进一个版本）
    → backfill_job（按来源水位生成待执行作业）
    → backfill_receipt（作业回执，保留被覆盖的事件范围）

约束：
  * 同一份提交（request_id / 同水位口径）只落一次记录，重复提交返回同一版本；
  * 每个迟到事件只允许归属一个旁路版本，重复到达不会产生第二个补数范围；
  * 补数范围必须覆盖该版本的水位区间，否则整组回退；
  * 两种口径（当前水位冻结 / 回看上一水位合并补报）各自成版本，
    写入迟到旁路数据当前结果，页面上再确定其中一份为可执行范围；
  * 冻结版本、生成作业、写回执都在 store.lock 内整组提交，失败整组回退。
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
from typing import Any

from app.store import store

SCOPES = ("current", "lookback")
SCOPE_LABELS = {
    "current": "当前水位冻结",
    "lookback": "回看上一水位合并补报",
}
VERSION_STATUS_ORDER = ["待确认", "待执行", "补数中", "已完成", "部分失败", "已作废"]
# 事务回退范围：冻结版本/生成作业/写回执会同时改这些表，失败时整组还原。
ROLLBACK_TABLES = (
    "watermark_track",
    "late_bypass",
    "late_bypass_event",
    "late_bypass_current",
    "backfill_job",
    "backfill_receipt",
    "late_todo",
    "late_submission",
)

# 迟到链路的事务锁与 store 共用一把可重入锁：同进程内整组提交/回退。
_LOCK: threading.RLock = store.lock


class LateBypassError(Exception):
    """带错误码的领域异常，路由层据此返回可读的失败原因。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------

def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _next_id(table: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(table)), default=0) + 1


def _stable_hash(*parts: Any) -> str:
    raw = "|".join(str(part) for part in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def _format_ts(value: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def _round(value: float, digits: int = 4) -> float:
    return round(value, digits)


def _snapshot() -> dict[str, list[dict[str, Any]]]:
    return {name: copy.deepcopy(store.rows(name)) for name in ROLLBACK_TABLES}


def _restore(snapshot: dict[str, list[dict[str, Any]]]) -> None:
    for name, rows in snapshot.items():
        store._tables[name] = rows


def _find_watermark(watermark_id: int) -> dict[str, Any]:
    record = store.find("watermark_track", watermark_id)
    if record is None:
        raise LateBypassError("watermark_not_found", f"水位记录 {watermark_id} 不存在")
    return record


def _policy_for_source(source: str) -> dict[str, Any] | None:
    for row in store.rows("late_handler"):
        if row.get("数据源") == source:
            return row
    return None


def watermark_interval(record: dict[str, Any], scope: str = "current") -> dict[str, float]:
    """水位区间：默认 (上一水位, 当前水位]；回看口径额外并入上一水位所在窗口。"""
    current = _to_float(record.get("当前水位"))
    previous = _to_float(record.get("_previous_watermark"), current)
    window = _to_float(record.get("_window_size"), 3600.0)
    if previous >= current:
        previous = current - window
    allowed = _to_float(record.get("延迟阈值"), 30.0)
    if scope == "lookback":
        start = previous - window
    else:
        start = previous
    return {
        "start": start,
        "end": current,
        "cutoff": current - allowed,
        "allowed": allowed,
        "window": window,
        "previous": previous,
    }


def _event_key(event: dict[str, Any]) -> str:
    return f"{event.get('数据源', '')}:{event.get('event_id', '')}"


def _bypassed_event_keys() -> set[str]:
    return {row.get("event_key", "") for row in store.rows("late_bypass_event")}


def _events_for_source(source: str, watermark_id: int | None = None) -> list[dict[str, Any]]:
    rows = []
    for row in store.rows("watermark_event"):
        if row.get("数据源") != source:
            continue
        if watermark_id is not None and int(row.get("watermark_id", 0)) != watermark_id:
            continue
        rows.append(row)
    rows.sort(key=lambda row: (_to_float(row.get("event_time")), str(row.get("event_id"))))
    return rows


def _dedupe_events(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """同一事件重复到达时只保留最早一条；重复项单独返回用于展示。"""
    seen: dict[str, dict[str, Any]] = {}
    repeated: list[dict[str, Any]] = []
    for event in events:
        key = _event_key(event)
        if key in seen:
            repeated.append(event)
            continue
        seen[key] = event
    return list(seen.values()), repeated


def _scope_events(record: dict[str, Any], scope: str) -> list[dict[str, Any]]:
    interval = watermark_interval(record, scope)
    source = record.get("数据源", "")
    events = _events_for_source(source)
    return [
        event for event in events
        if interval["start"] < _to_float(event.get("event_time")) <= interval["end"]
    ]


def _ratio(part: int, whole: int) -> float:
    return _round(part / whole, 4) if whole else 0.0


def _threshold_for(source: str) -> float:
    policy = _policy_for_source(source) or {}
    raw = _to_float(policy.get("阈值百分比"), 20.0)
    return _round(raw / 100, 4)


def _version_by_dedupe_key(key: str) -> dict[str, Any] | None:
    for row in store.rows("late_bypass"):
        if row.get("_dedupe_key") == key and row.get("status") != "已作废":
            return row
    return None


def _version_by_request(
    request_id: str | None, scope: str | None = None,
) -> dict[str, Any] | None:
    """同一提交按 (request_id, 口径) 识别；整组提交里两个口径各落一条。"""
    if not request_id:
        return None
    for row in store.rows("late_bypass"):
        if row.get("_request_id") != request_id:
            continue
        if scope is not None and row.get("口径") != scope:
            continue
        if row.get("status") != "已作废":
            return row
    return None


def _events_of_version(version_id: int) -> list[dict[str, Any]]:
    rows = [
        row for row in store.rows("late_bypass_event")
        if int(row.get("bypass_id", 0)) == version_id
    ]
    rows.sort(key=lambda row: _to_float(row.get("event_time")))
    return rows


def _job_of_version(version: dict[str, Any]) -> dict[str, Any] | None:
    job_id = version.get("_job_id")
    if job_id is None:
        return None
    for row in store.rows("backfill_job"):
        if int(row.get("id", 0)) == int(job_id):
            return row
    return None


def _receipt_of_version(version_id: int) -> list[dict[str, Any]]:
    return [
        row for row in store.rows("backfill_receipt")
        if int(row.get("旁路版本ID", 0)) == version_id
    ]


def _version_public(version: dict[str, Any]) -> dict[str, Any]:
    """对外结构：保留列表需要的字段，去掉内部快照。"""
    public = {key: value for key, value in version.items() if not key.startswith("_")}
    job = _job_of_version(version)
    receipts = _receipt_of_version(int(version.get("id", 0)))
    public["作业ID"] = job.get("id") if job else None
    public["作业状态"] = job.get("status") if job else "未生成"
    public["作业进度"] = job.get("进度比例") if job else "—"
    public["回执数"] = len(receipts)
    public["最近回执"] = (
        {key: value for key, value in receipts[-1].items() if not key.startswith("_")}
        if receipts else None
    )
    return public


# ---------------------------------------------------------------------------
# 迟到识别 / 分布
# ---------------------------------------------------------------------------

def analyse_late(
    record: dict[str, Any],
    scope: str = "current",
) -> dict[str, Any]:
    """识别某条水位记录在指定口径下的迟到事件。"""
    if scope not in SCOPES:
        raise LateBypassError("invalid_scope", f"未知口径：{scope}")
    interval = watermark_interval(record, scope)
    raw_events = _scope_events(record, scope)
    unique_events, repeated = _dedupe_events(raw_events)
    late = [e for e in unique_events if _to_float(e.get("event_time")) < interval["cutoff"]]
    on_time = [e for e in unique_events if _to_float(e.get("event_time")) >= interval["cutoff"]]
    bypassed_keys = _bypassed_event_keys()
    already = [e for e in late if _event_key(e) in bypassed_keys]
    candidates = [e for e in late if _event_key(e) not in bypassed_keys]

    threshold = _threshold_for(record.get("数据源", ""))
    late_ratio = _ratio(len(late), len(unique_events))
    lateness_values = [
        _round(interval["cutoff"] - _to_float(e.get("event_time")), 1) for e in late
    ]
    buckets = _build_buckets(late, interval["cutoff"], interval["allowed"])
    return {
        "scope": scope,
        "scope_label": SCOPE_LABELS[scope],
        "interval": interval,
        "interval_label": f"{int(interval['start'])}-{int(interval['end'])}",
        "events": unique_events,
        "late_events": late,
        "on_time_events": on_time,
        "duplicate_arrivals": repeated,
        "already_bypassed": already,
        "candidates": candidates,
        "late_count": len(late),
        "interval_event_count": len(unique_events),
        "duplicate_count": len(repeated),
        "already_bypassed_count": len(already),
        "candidate_count": len(candidates),
        "late_ratio": late_ratio,
        "max_lateness": max(lateness_values) if lateness_values else 0.0,
        "avg_lateness": (
            _round(sum(lateness_values) / len(lateness_values), 1) if lateness_values else 0.0
        ),
        "threshold": threshold,
        "threshold_hit": late_ratio > threshold and bool(late),
        "buckets": buckets,
        "event_ids": [str(e.get("event_id", "")) for e in late],
    }


def _build_buckets(
    late_events: list[dict[str, Any]], cutoff: float, allowed: float,
) -> list[dict[str, Any]]:
    """按「迟到了几个允许延迟」分桶，供水位页面的迟到分布图使用。"""
    unit = allowed if allowed > 0 else 60.0
    bucket_specs = [
        ("0-1x", 0.0, 1.0),
        ("1-2x", 1.0, 2.0),
        ("2-5x", 2.0, 5.0),
        ("5x+", 5.0, None),
    ]
    buckets = []
    for label, low, high in bucket_specs:
        matched = []
        for event in late_events:
            ratio = (cutoff - _to_float(event.get("event_time"))) / unit
            if ratio < low:
                continue
            if high is not None and ratio >= high:
                continue
            matched.append(event)
        buckets.append({
            "bucket": label,
            "count": len(matched),
            "event_ids": [str(e.get("event_id", "")) for e in matched],
        })
    return buckets


def late_distribution(watermark_id: int) -> dict[str, Any]:
    """水位详情页：迟到分布 + 两种口径的条件范围 + 规则命中。"""
    record = _find_watermark(watermark_id)
    scopes = {scope: analyse_late(record, scope) for scope in SCOPES}
    versions = [
        _version_public(row) for row in store.rows("late_bypass")
        if int(row.get("水位记录ID", 0)) == watermark_id
        and row.get("status") != "已作废"
    ]
    policy = _policy_for_source(record.get("数据源", "")) or {}
    current = scopes["current"]
    rule_hit = {
        "rule": "迟到率阈值判定",
        "policy": policy.get("策略名称", "未绑定策略"),
        "allowed_lateness": current["interval"]["allowed"],
        "threshold": current["threshold"],
        "late_ratio": current["late_ratio"],
        "matched": current["threshold_hit"],
        "reason": (
            f"迟到率 {current['late_ratio']:.2%} 高于阈值 {current['threshold']:.2%}"
            if current["threshold_hit"]
            else f"迟到率 {current['late_ratio']:.2%} 未超过阈值 {current['threshold']:.2%}"
        ),
        "condition_range": {
            "watermark": int(current["interval"]["end"]),
            "cutoff": int(current["interval"]["cutoff"]),
            "start": int(current["interval"]["start"]),
            "end": int(current["interval"]["end"]),
            "expression": (
                f"event_time in ({int(current['interval']['start'])}, "
                f"{int(current['interval']['end'])}] and event_time < "
                f"{int(current['interval']['cutoff'])}"
            ),
        },
    }
    return {
        "watermark": {key: value for key, value in record.items() if not key.startswith("_")},
        "policy": {key: value for key, value in policy.items() if not key.startswith("_")},
        "scopes": {
            scope: {
                key: value for key, value in analysis.items()
                if key not in ("events", "late_events", "on_time_events",
                               "candidate_events", "candidates")
            }
            | {
                "late_events": [
                    _event_public(event, analysis["interval"]["cutoff"])
                    for event in analysis["late_events"]
                ],
                "candidates": [
                    _event_public(event, analysis["interval"]["cutoff"])
                    for event in analysis["candidates"]
                ],
            }
            for scope, analysis in scopes.items()
        },
        "distribution": current["buckets"],
        "rule_hit": rule_hit,
        "versions": versions,
        "current_result": current_result_for(record.get("数据源", "")),
    }


def _event_public(event: dict[str, Any], cutoff: float) -> dict[str, Any]:
    return {
        "event_id": event.get("event_id", ""),
        "数据源": event.get("数据源", ""),
        "event_time": _round(_to_float(event.get("event_time")), 1),
        "event_time_iso": event.get("event_time_iso", ""),
        "arrival_time": _round(_to_float(event.get("arrival_time")), 1),
        "arrival_time_iso": event.get("arrival_time_iso", ""),
        "lateness": _round(cutoff - _to_float(event.get("event_time")), 1),
    }


# ---------------------------------------------------------------------------
# 冻结旁路版本
# ---------------------------------------------------------------------------

def freeze_version(
    watermark_id: int,
    scope: str = "current",
    *,
    request_id: str | None = None,
    operator: str = "责任审核人",
    inject_failure: bool = False,
) -> dict[str, Any]:
    """读取水位记录、识别迟到事件并冻结一份旁路版本。

    幂等：同一 request_id 或同一水位口径（水位区间 + 截止点）只落一次记录。
    """
    if scope not in SCOPES:
        raise LateBypassError("invalid_scope", f"未知口径：{scope}")

    with _LOCK:
        record = _find_watermark(watermark_id)
        existing = _version_by_request(request_id, scope)
        if existing is not None:
            return {
                "ok": True,
                "created": False,
                "idempotent": True,
                "reason": "同一提交已落过记录",
                "version": _version_public(existing),
            }

        interval = watermark_interval(record, scope)
        dedupe_key = _stable_hash(
            record.get("数据源", ""), scope,
            int(interval["start"]), int(interval["end"]), int(interval["cutoff"]),
        )
        existing = _version_by_dedupe_key(dedupe_key)
        if existing is not None:
            return {
                "ok": True,
                "created": False,
                "idempotent": True,
                "reason": "该水位区间与口径已经冻结过旁路版本",
                "version": _version_public(existing),
            }

        analysis = analyse_late(record, scope)
        if not analysis["candidates"]:
            return {
                "ok": True,
                "created": False,
                "idempotent": True,
                "reason": (
                    "迟到事件已全部旁路，未新增补数范围"
                    if analysis["late_count"] else "当前水位没有迟到事件"
                ),
                "version": None,
                "late_count": analysis["late_count"],
                "already_bypassed_count": analysis["already_bypassed_count"],
            }

        snapshot = _snapshot()
        try:
            version_id = _next_id("late_bypass")
            seq = sum(
                1 for row in store.rows("late_bypass")
                if row.get("数据源") == record.get("数据源")
            ) + 1
            version = {
                "id": version_id,
                "版本号": f"LB-{record.get('数据源', 'src')}-{seq:03d}",
                "数据源": record.get("数据源", ""),
                "水位记录ID": watermark_id,
                "水位名称": record.get("水位名称", ""),
                "口径": scope,
                "口径名称": SCOPE_LABELS[scope],
                "水位区间": analysis["interval_label"],
                "区间起始": int(interval["start"]),
                "区间结束": int(interval["end"]),
                "迟到截止": int(interval["cutoff"]),
                "允许延迟": int(interval["allowed"]),
                "补数范围": analysis["interval_label"],
                "范围起始": int(interval["start"]),
                "范围结束": int(interval["end"]),
                "迟到事件数": analysis["late_count"],
                "区间事件数": analysis["interval_event_count"],
                "重复到达数": analysis["duplicate_count"],
                "已旁路跳过数": analysis["already_bypassed_count"],
                "迟到率": analysis["late_ratio"],
                "阈值百分比": _round(analysis["threshold"] * 100, 2),
                "阈值命中": "是" if analysis["threshold_hit"] else "否",
                "最大迟到秒": analysis["max_lateness"],
                "事件ID列表": analysis["event_ids"],
                "status": "待确认",
                "pending": True,
                "abnormal": analysis["threshold_hit"],
                "created_at": time.time(),
                "_dedupe_key": dedupe_key,
                "_request_id": request_id or "",
                "_operator": operator,
                "_attempt": 1,
                "_job_id": None,
                "_events_snapshot": [
                    _event_public(event, interval["cutoff"]) for event in analysis["candidates"]
                ],
            }
            store.rows("late_bypass").append(version)

            for event in analysis["candidates"]:
                store.rows("late_bypass_event").append({
                    "id": _next_id("late_bypass_event"),
                    "event_key": _event_key(event),
                    "bypass_id": version_id,
                    "event_id": event.get("event_id", ""),
                    "数据源": event.get("数据源", ""),
                    "event_time": _to_float(event.get("event_time")),
                    "arrival_time": _to_float(event.get("arrival_time")),
                    "lateness": _round(
                        interval["cutoff"] - _to_float(event.get("event_time")), 1
                    ),
                    "status": "已旁路",
                    "pending": True,
                    "abnormal": False,
                })

            _upsert_current_result(version, executable=False)
            _sync_watermark_record(watermark_id)
            _add_todo(
                "watermark_track",
                f"{version['版本号']} 待确认可执行补数范围",
                watermark_id=watermark_id,
                version_id=version_id,
            )

            if inject_failure:
                # 模拟事务中途失败：整组回退，不留下半个版本。
                raise LateBypassError("transaction_failed", "冻结旁路版本时事务失败")

            return {
                "ok": True,
                "created": True,
                "idempotent": False,
                "version": _version_public(version),
            }
        except LateBypassError:
            _restore(snapshot)
            raise
        except Exception as exc:  # pragma: no cover - 兜底
            _restore(snapshot)
            raise LateBypassError("transaction_failed", f"冻结旁路版本失败：{exc}") from exc


def _sync_watermark_record(watermark_id: int) -> None:
    """把旁路版本/作业概况回写到水位记录，其他页面据此显示待办。"""
    record = store.find("watermark_track", watermark_id)
    if record is None:
        return
    versions = [
        row for row in store.rows("late_bypass")
        if int(row.get("水位记录ID", 0)) == watermark_id and row.get("status") != "已作废"
    ]
    bypassed_keys = {
        row.get("event_key")
        for row in store.rows("late_bypass_event")
        if int(row.get("bypass_id", 0)) in {int(v.get("id", 0)) for v in versions}
    }
    analysis = analyse_late(record, "current")
    record["旁路版本数"] = len(versions)
    record["旁路版本"] = "、".join(str(v.get("版本号", "")) for v in versions)
    record["已旁路事件数"] = sum(
        1 for event in analysis["events"] if _event_key(event) in bypassed_keys
    )
    record["待旁路事件数"] = analysis["candidate_count"]
    record["迟到事件数"] = analysis["late_count"]
    record["迟到率"] = analysis["late_ratio"]
    record["阈值命中"] = "是" if analysis["threshold_hit"] else "否"
    has_running = any(v.get("status") in ("待确认", "待执行", "补数中") for v in versions)
    has_versions = bool(versions)
    record["pending"] = has_running
    record["abnormal"] = bool(record.get("abnormal")) or (
        analysis["threshold_hit"] and has_versions
    )


def _add_todo(
    module: str,
    title: str,
    *,
    watermark_id: int | None = None,
    version_id: int | None = None,
    job_id: int | None = None,
    status: str = "待处理",
) -> dict[str, Any]:
    """给其他页面生成待办：同一业务对象只保留一条未完成待办。"""
    rows = store.rows("late_todo")
    for row in rows:
        if (
            row.get("module") == module
            and row.get("watermark_id") == watermark_id
            and row.get("version_id") == version_id
            and row.get("job_id") == job_id
            and row.get("status") == "待处理"
        ):
            row["title"] = title
            row["updated_at"] = time.time()
            return row
    todo = {
        "id": _next_id("late_todo"),
        "module": module,
        "title": title,
        "watermark_id": watermark_id,
        "version_id": version_id,
        "job_id": job_id,
        "status": status,
        "pending": status == "待处理",
        "abnormal": False,
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    rows.append(todo)
    return todo


def _close_todos(module: str, **match: Any) -> None:
    for row in store.rows("late_todo"):
        if row.get("module") != module or row.get("status") != "待处理":
            continue
        if all(row.get(key) == value for key, value in match.items()):
            row["status"] = "已处理"
            row["pending"] = False
            row["updated_at"] = time.time()


def list_todos(module: str | None = None) -> list[dict[str, Any]]:
    rows = list(store.rows("late_todo"))
    if module:
        rows = [row for row in rows if row.get("module") == module]
    rows.sort(key=lambda row: int(row.get("id", 0)), reverse=True)
    return rows


def _upsert_current_result(version: dict[str, Any], *, executable: bool) -> dict[str, Any]:
    """写入迟到旁路数据当前结果：每个 (数据源, 口径) 只保留一个当前版本。"""
    source = version.get("数据源", "")
    scope = version.get("口径", "current")
    rows = store.rows("late_bypass_current")
    current = next(
        (row for row in rows
         if row.get("数据源") == source and row.get("口径") == scope),
        None,
    )
    payload = {
        "数据源": source,
        "口径": scope,
        "口径名称": SCOPE_LABELS.get(scope, scope),
        "当前版本ID": version.get("id"),
        "当前版本号": version.get("版本号"),
        "水位记录ID": version.get("水位记录ID"),
        "水位区间": version.get("水位区间"),
        "补数范围": version.get("补数范围"),
        "可执行": bool(executable),
        "更新时间": time.time(),
        "status": "当前结果" if executable else "候选结果",
        "pending": not executable,
        "abnormal": False,
    }
    if current is None:
        payload["id"] = _next_id("late_bypass_current")
        rows.append(payload)
        return payload
    current.update(payload)
    return current


def current_result_for(source: str) -> list[dict[str, Any]]:
    rows = [
        row for row in store.rows("late_bypass_current")
        if row.get("数据源") == source
    ]
    return [{key: value for key, value in row.items() if not key.startswith("_")} for row in rows]


# ---------------------------------------------------------------------------
# 确认可执行范围 / 生成补数作业
# ---------------------------------------------------------------------------

def confirm_version(
    version_id: int,
    *,
    request_id: str | None = None,
    operator: str = "责任审核人",
    inject_failure: bool = False,
) -> dict[str, Any]:
    """确定一份可执行补数范围，并生成待执行补数作业。"""
    with _LOCK:
        version = store.find("late_bypass", version_id)
        if version is None:
            raise LateBypassError("version_not_found", f"旁路版本 {version_id} 不存在")
        if version.get("status") == "已作废":
            raise LateBypassError("version_void", "旁路版本已作废，不能确认")

        job = _job_of_version(version)
        if job is not None and version.get("status") in ("待执行", "补数中", "已完成"):
            version["_confirm_request"] = request_id or version.get("_confirm_request", "")
            return {
                "ok": True,
                "created": False,
                "idempotent": True,
                "reason": "该版本已经确认过补数范围",
                "version": _version_public(version),
                "job": {key: value for key, value in job.items() if not key.startswith("_")},
            }

        interval_start = int(version.get("区间起始", 0))
        interval_end = int(version.get("区间结束", 0))
        job_start = min(
            [interval_start] + [
                int(event.get("event_time", interval_start))
                for event in _events_of_version(version_id)
            ]
        )
        job_end = max(
            [interval_end] + [
                int(event.get("event_time", interval_end))
                for event in _events_of_version(version_id)
            ]
        )
        if not (job_start <= interval_start and job_end >= interval_end):
            raise LateBypassError(
                "range_not_covered",
                f"补数范围 {job_start}-{job_end} 未覆盖水位区间 "
                f"{interval_start}-{interval_end}",
            )

        snapshot = _snapshot()
        try:
            job_row = {
                "id": _next_id("backfill_job"),
                "作业名称": (
                    f"迟到补数_{version.get('数据源', '')}_"
                    f"{version.get('版本号', '')}_{int(time.time())}"
                ),
                "目标指标": f"{version.get('数据源', '')}_metrics",
                "补数范围": f"{job_start}-{job_end}",
                "并行度": str(min(max(int(version.get("迟到事件数", 1)), 1), 8)),
                "起始位点": str(job_start),
                "截止位点": str(job_end),
                "进度比例": "0%",
                "status": "待执行",
                "pending": True,
                "abnormal": False,
                "_bypass_id": version_id,
                "_bypass_version": version.get("版本号"),
                "_source_watermark": version.get("水位记录ID"),
                "_watermark_interval": version.get("水位区间"),
                "_condition_range": f"{interval_start}-{interval_end}",
                "_covered_event_ids": list(version.get("事件ID列表", [])),
                "_covered_event_range": _covered_range(version_id),
                "_confirm_request": request_id or "",
            }
            store.rows("backfill_job").append(job_row)

            version["_job_id"] = job_row["id"]
            version["补数范围"] = job_row["补数范围"]
            version["范围起始"] = job_start
            version["范围结束"] = job_end
            version["status"] = "待执行"
            version["pending"] = True
            version["_confirm_request"] = request_id or ""

            # 同一数据源最终只保留一份可执行范围，其余版本降为候选。
            for row in store.rows("late_bypass_current"):
                if row.get("数据源") == version.get("数据源"):
                    if row.get("口径") == version.get("口径"):
                        row.update({
                            "当前版本ID": version.get("id"),
                            "当前版本号": version.get("版本号"),
                            "水位区间": version.get("水位区间"),
                            "补数范围": version.get("补数范围"),
                            "可执行": True,
                            "status": "当前结果",
                            "pending": False,
                            "更新时间": time.time(),
                        })
                    else:
                        row.update({
                            "可执行": False,
                            "status": "候选结果",
                            "pending": True,
                            "更新时间": time.time(),
                        })

            _sync_watermark_record(int(version.get("水位记录ID", 0)))
            _add_todo(
                "backfill_job",
                f"{version.get('版本号')} 已生成补数作业，等待执行",
                watermark_id=version.get("水位记录ID"),
                version_id=version_id,
                job_id=job_row["id"],
            )
            _close_todos("watermark_track", version_id=version_id)

            if inject_failure:
                raise LateBypassError("transaction_failed", "生成补数作业时事务失败")

            return {
                "ok": True,
                "created": True,
                "idempotent": False,
                "version": _version_public(version),
                "job": {key: value for key, value in job_row.items() if not key.startswith("_")},
                "range_covers_interval": True,
            }
        except LateBypassError:
            _restore(snapshot)
            raise
        except Exception as exc:  # pragma: no cover - 兜底
            _restore(snapshot)
            raise LateBypassError("transaction_failed", f"生成补数作业失败：{exc}") from exc


def _covered_range(version_id: int) -> str:
    events = _events_of_version(version_id)
    if not events:
        return ""
    times = [int(_to_float(event.get("event_time"))) for event in events]
    return f"{min(times)}-{max(times)}"


# ---------------------------------------------------------------------------
# 执行补数 / 作业回执 / 故障重开
# ---------------------------------------------------------------------------

def execute_job(
    job_id: int,
    *,
    request_id: str | None = None,
    operator: str = "责任审核人",
    inject_failure: bool = False,
) -> dict[str, Any]:
    """补数任务读取迟到旁路数据并生成回执，回执保留被覆盖的事件范围。"""
    with _LOCK:
        job = store.find("backfill_job", job_id)
        if job is None:
            raise LateBypassError("job_not_found", f"补数作业 {job_id} 不存在")
        version_id = job.get("_bypass_id")
        if version_id is None:
            raise LateBypassError("not_bypass_job", "该补数作业不是迟到旁路生成的")
        version = store.find("late_bypass", int(version_id))
        if version is None:
            raise LateBypassError("version_not_found", f"旁路版本 {version_id} 不存在")

        existing = [
            row for row in store.rows("backfill_receipt")
            if int(row.get("作业ID", 0)) == job_id
            and row.get("_request_id") == (request_id or "")
        ]
        if existing:
            return {
                "ok": True,
                "created": False,
                "idempotent": True,
                "reason": "同一请求已生成过回执",
                "receipt": {key: value for key, value in existing[-1].items()
                            if not key.startswith("_")},
                "version": _version_public(version),
            }

        if version.get("status") == "已完成":
            receipts = _receipt_of_version(int(version_id))
            return {
                "ok": True,
                "created": False,
                "idempotent": True,
                "reason": "该旁路版本已完成补数",
                "receipt": (
                    {key: value for key, value in receipts[-1].items()
                     if not key.startswith("_")}
                    if receipts else None
                ),
                "version": _version_public(version),
            }

        snapshot = _snapshot()
        try:
            events = _events_of_version(int(version_id))
            if not events:
                raise LateBypassError("empty_version", "旁路版本没有可补数事件")
            covered_ids = [str(event.get("event_id", "")) for event in events]
            covered_range = _covered_range(int(version_id))
            attempt = int(version.get("_attempt", 1))

            job["status"] = "补数中"
            job["pending"] = True
            version["status"] = "补数中"
            version["pending"] = True

            if inject_failure:
                # 模拟回执落库失败：作业和版本状态一起回退，口径不变。
                raise LateBypassError("transaction_failed", "写作业回执时事务失败")

            receipt = {
                "id": _next_id("backfill_receipt"),
                "回执号": f"RCPT-{job_id}-{attempt:02d}",
                "作业ID": job_id,
                "作业名称": job.get("作业名称", ""),
                "旁路版本ID": int(version_id),
                "版本号": version.get("版本号", ""),
                "数据源": version.get("数据源", ""),
                "水位记录ID": version.get("水位记录ID"),
                "口径": version.get("口径", ""),
                "口径名称": version.get("口径名称", ""),
                "水位区间": version.get("水位区间", ""),
                "覆盖事件范围": covered_range,
                "覆盖事件数": len(covered_ids),
                "覆盖事件ID列表": covered_ids,
                "补数范围": job.get("补数范围", ""),
                "尝试次数": attempt,
                "状态": "已完成",
                "status": "已完成",
                "pending": False,
                "abnormal": False,
                "executed_at": time.time(),
                "executed_at_text": _format_ts(time.time()),
                "_request_id": request_id or "",
            }
            store.rows("backfill_receipt").append(receipt)

            job["status"] = "已完成"
            job["pending"] = False
            job["进度比例"] = "100%"
            job["_receipt_id"] = receipt["id"]
            version["status"] = "已完成"
            version["pending"] = False
            version["abnormal"] = False
            version["_receipt_id"] = receipt["id"]
            for link in store.rows("late_bypass_event"):
                if int(link.get("bypass_id", 0)) == int(version_id):
                    link["status"] = "已补数"
                    link["pending"] = False
            _sync_watermark_record(int(version.get("水位记录ID", 0)))
            _close_todos("backfill_job", job_id=job_id)

            return {
                "ok": True,
                "created": True,
                "idempotent": False,
                "receipt": {key: value for key, value in receipt.items()
                            if not key.startswith("_")},
                "version": _version_public(version),
            }
        except LateBypassError:
            _restore(snapshot)
            raise
        except Exception as exc:  # pragma: no cover - 兜底
            _restore(snapshot)
            raise LateBypassError("transaction_failed", f"执行补数失败：{exc}") from exc


def reopen_version(
    version_id: int,
    *,
    request_id: str | None = None,
    operator: str = "责任审核人",
) -> dict[str, Any]:
    """故障重开：沿用同一口径、同一范围重试，不新增旁路版本。

    重复重试只记一次：同一 request_id 再次调用返回同一尝试。
    """
    with _LOCK:
        version = store.find("late_bypass", version_id)
        if version is None:
            raise LateBypassError("version_not_found", f"旁路版本 {version_id} 不存在")
        if version.get("_reopen_request") and version.get("_reopen_request") == request_id:
            return {
                "ok": True,
                "created": False,
                "idempotent": True,
                "reason": "同一重开请求已记录",
                "version": _version_public(version),
            }
        if version.get("status") not in ("部分失败", "已完成"):
            raise LateBypassError(
                "reopen_not_allowed",
                f"版本状态为「{version.get('status')}」，只有部分失败/已完成版本可重开",
            )

        version["_attempt"] = int(version.get("_attempt", 1)) + 1
        version["status"] = "待执行"
        version["pending"] = True
        version["abnormal"] = False
        version["_reopen_request"] = request_id or ""
        version["_reopened_at"] = time.time()
        job = _job_of_version(version)
        if job is not None:
            job["status"] = "待执行"
            job["pending"] = True
            job["进度比例"] = "0%"
        return {
            "ok": True,
            "created": True,
            "idempotent": False,
            "reason": "已按同一口径重开",
            "version": _version_public(version),
        }


def mark_partial_failure(version_id: int, reason: str = "补数执行失败") -> None:
    version = store.find("late_bypass", version_id)
    if version is None:
        return
    version["status"] = "部分失败"
    version["pending"] = True
    version["abnormal"] = True
    version["_failure_reason"] = reason
    job = _job_of_version(version)
    if job is not None:
        job["status"] = "部分失败"
        job["pending"] = True
        job["abnormal"] = True


# ---------------------------------------------------------------------------
# 查询视图
# ---------------------------------------------------------------------------

def list_versions(
    *,
    source: str | None = None,
    watermark_id: int | None = None,
    status: str | None = None,
    scope: str | None = None,
) -> list[dict[str, Any]]:
    rows = list(store.rows("late_bypass"))
    if source:
        rows = [row for row in rows if row.get("数据源") == source]
    if watermark_id is not None:
        rows = [row for row in rows if int(row.get("水位记录ID", 0)) == watermark_id]
    if status:
        rows = [row for row in rows if row.get("status") == status]
    if scope:
        rows = [row for row in rows if row.get("口径") == scope]
    rows.sort(key=lambda row: int(row.get("id", 0)), reverse=True)
    return [_version_public(row) for row in rows]


def version_detail(version_id: int) -> dict[str, Any]:
    version = store.find("late_bypass", version_id)
    if version is None:
        raise LateBypassError("version_not_found", f"旁路版本 {version_id} 不存在")
    job = _job_of_version(version)
    events = _events_of_version(version_id)
    receipts = _receipt_of_version(version_id)
    watermark_record = store.find("watermark_track", int(version.get("水位记录ID", 0)))
    analysis = (
        analyse_late(watermark_record, version.get("口径", "current"))
        if watermark_record else None
    )
    return {
        "version": _version_public(version),
        "rule_hit": {
            "matched": bool(analysis and analysis["threshold_hit"]),
            "policy": (
                (_policy_for_source(version.get("数据源", "")) or {}).get("策略名称", "")
            ),
            "threshold": analysis["threshold"] if analysis else 0.0,
            "late_ratio": analysis["late_ratio"] if analysis else 0.0,
            "late_count": analysis["late_count"] if analysis else 0,
            "reason": (
                f"迟到率 {analysis['late_ratio']:.2%} 与阈值 {analysis['threshold']:.2%} "
                f"{'命中' if analysis['threshold_hit'] else '未命中'}"
                if analysis else "水位记录不存在"
            ),
        } if analysis else None,
        "watermark": (
            {key: value for key, value in watermark_record.items() if not key.startswith("_")}
            if watermark_record else None
        ),
        "events": [
            {key: value for key, value in event.items() if not key.startswith("_")}
            for event in events
        ],
        "job": {key: value for key, value in job.items() if not key.startswith("_")} if job else None,
        "receipts": [
            {key: value for key, value in receipt.items() if not key.startswith("_")}
            for receipt in receipts
        ],
        "range_covers_interval": bool(job) and (
            int(_to_float(job.get("起始位点")))
            <= int(version.get("区间起始", 0))
            and int(_to_float(job.get("截止位点")))
            >= int(version.get("区间结束", 0))
        ),
    }


def list_jobs(source: str | None = None) -> list[dict[str, Any]]:
    """补数任务页面：每个旁路版本对应的作业、覆盖范围与完成状态。"""
    jobs = []
    for job in store.rows("backfill_job"):
        bypass_id = job.get("_bypass_id")
        if bypass_id is None:
            continue
        version = store.find("late_bypass", int(bypass_id))
        if version is None:
            continue
        if source and version.get("数据源") != source:
            continue
        receipts = [
            receipt for receipt in store.rows("backfill_receipt")
            if int(receipt.get("作业ID", 0)) == int(job.get("id", 0))
        ]
        jobs.append({
            "作业ID": job.get("id"),
            "作业名称": job.get("作业名称", ""),
            "作业状态": job.get("status", ""),
            "进度比例": job.get("进度比例", ""),
            "补数范围": job.get("补数范围", ""),
            "旁路版本ID": int(bypass_id),
            "版本号": version.get("版本号", ""),
            "数据源": version.get("数据源", ""),
            "水位记录ID": version.get("水位记录ID"),
            "口径": version.get("口径", ""),
            "口径名称": version.get("口径名称", ""),
            "水位区间": version.get("水位区间", ""),
            "范围覆盖": (
                "覆盖" if int(_to_float(job.get("起始位点")))
                <= int(version.get("区间起始", 0))
                and int(_to_float(job.get("截止位点")))
                >= int(version.get("区间结束", 0))
                else "未覆盖"
            ),
            "覆盖事件范围": job.get("_covered_event_range", ""),
            "覆盖事件数": len(job.get("_covered_event_ids", [])),
            "回执数": len(receipts),
            "最近回执号": receipts[-1].get("回执号") if receipts else "",
            "完成状态": job.get("status", ""),
            "规则命中": version.get("阈值命中", ""),
            "迟到率": version.get("迟到率", 0.0),
            "阈值百分比": version.get("阈值百分比", 0.0),
        })
    jobs.sort(key=lambda item: int(item.get("作业ID", 0)), reverse=True)
    return jobs


def list_receipts(version_id: int | None = None, job_id: int | None = None) -> list[dict[str, Any]]:
    rows = list(store.rows("backfill_receipt"))
    if version_id is not None:
        rows = [row for row in rows if int(row.get("旁路版本ID", 0)) == version_id]
    if job_id is not None:
        rows = [row for row in rows if int(row.get("作业ID", 0)) == job_id]
    rows.sort(key=lambda row: int(row.get("id", 0)), reverse=True)
    return [{key: value for key, value in row.items() if not key.startswith("_")} for row in rows]


# ---------------------------------------------------------------------------
# 阈值核对
# ---------------------------------------------------------------------------

def audit(
    *,
    watermark_id: int | None = None,
    start: float | None = None,
    end: float | None = None,
) -> dict[str, Any]:
    """按水位区间核对：迟到旁路数据是否齐全、补数清单是否覆盖水位区间。"""
    records = list(store.rows("watermark_track"))
    if watermark_id is not None:
        records = [row for row in records if int(row.get("id", 0)) == watermark_id]
    rows = []
    for record in records:
        interval = watermark_interval(record, "current")
        if start is not None and interval["end"] < start:
            continue
        if end is not None and interval["start"] > end:
            continue
        analysis = analyse_late(record, "current")
        versions = [
            row for row in store.rows("late_bypass")
            if int(row.get("水位记录ID", 0)) == int(record.get("id", 0))
            and row.get("status") != "已作废"
            and int(row.get("区间结束", 0)) >= int(interval["start"])
            and int(row.get("区间起始", 0)) <= int(interval["end"])
        ]
        bypassed_ids: set[str] = set()
        for version in versions:
            bypassed_ids.update(str(item) for item in version.get("事件ID列表", []))
        expected_ids = {str(event.get("event_id", "")) for event in analysis["late_events"]}
        uncovered = sorted(expected_ids - bypassed_ids)
        jobs = [
            {
                "作业ID": job.get("id"),
                "作业状态": job.get("status", ""),
                "补数范围": job.get("补数范围", ""),
                "版本号": job.get("_bypass_version", ""),
                "覆盖": (
                    int(_to_float(job.get("起始位点")))
                    <= int(interval["start"])
                    and int(_to_float(job.get("截止位点")))
                    >= int(interval["end"])
                ),
                "回执数": sum(
                    1 for receipt in store.rows("backfill_receipt")
                    if int(receipt.get("作业ID", 0)) == int(job.get("id", 0))
                ),
            }
            for job in store.rows("backfill_job")
            if job.get("_bypass_id") is not None
            and any(
                int(job["_bypass_id"]) == int(version.get("id", 0))
                for version in versions
            )
        ]
        jobs_covered = bool(jobs) and all(item["覆盖"] for item in jobs)
        rows.append({
            "水位记录ID": record.get("id"),
            "水位名称": record.get("水位名称", ""),
            "数据源": record.get("数据源", ""),
            "水位": int(interval["end"]),
            "水位区间": f"{int(interval['start'])}-{int(interval['end'])}",
            "迟到截止": int(interval["cutoff"]),
            "迟到事件数": analysis["late_count"],
            "已旁路事件数": len(expected_ids & bypassed_ids),
            "未旁路事件数": len(uncovered),
            "未旁路事件": uncovered,
            "迟到率": analysis["late_ratio"],
            "阈值百分比": _round(analysis["threshold"] * 100, 2),
            "阈值命中": "是" if analysis["threshold_hit"] else "否",
            "旁路版本数": len(versions),
            "旁路版本": [version.get("版本号", "") for version in versions],
            "补数作业数": len(jobs),
            "补数作业": jobs,
            "补数范围覆盖水位区间": jobs_covered,
            "state": (
                "缺旁路" if uncovered
                else "无需补数" if not expected_ids and not versions
                else "待冻结旁路" if not versions
                else "待生成补数" if not jobs
                else "一致" if jobs_covered
                else "补数范围未覆盖"
            ),
            "status": "正常" if not uncovered and jobs_covered else "待处理",
            "pending": bool(uncovered) or (bool(expected_ids) and not jobs_covered),
            "abnormal": bool(uncovered),
        })
    rows.sort(key=lambda item: int(item.get("水位记录ID", 0)))
    summary = {
        "checked": len(rows),
        "consistent": sum(1 for row in rows if row["state"] in ("一致", "无需补数")),
        "uncovered_events": sum(int(row["未旁路事件数"]) for row in rows),
        "jobs": sum(int(row["补数作业数"]) for row in rows),
        "threshold_hits": sum(1 for row in rows if row["阈值命中"] == "是"),
    }
    return {"summary": summary, "rows": rows}


def summary() -> dict[str, Any]:
    versions = store.rows("late_bypass")
    jobs = [
        job for job in store.rows("backfill_job") if job.get("_bypass_id") is not None
    ]
    receipts = store.rows("backfill_receipt")
    return {
        "watermarks": len(store.rows("watermark_track")),
        "versions": len(versions),
        "pending_versions": sum(1 for row in versions if row.get("status") == "待确认"),
        "executable_ranges": sum(
            1 for row in store.rows("late_bypass_current") if row.get("可执行")
        ),
        "bypassed_events": len(store.rows("late_bypass_event")),
        "jobs": len(jobs),
        "completed_jobs": sum(1 for job in jobs if job.get("status") == "已完成"),
        "receipts": len(receipts),
        "threshold_hits": sum(1 for row in versions if row.get("阈值命中") == "是"),
        "scopes": {
            scope: sum(1 for row in versions if row.get("口径") == scope)
            for scope in SCOPES
        },
    }


# ---------------------------------------------------------------------------
# 整组提交：两种口径一次提交，只落一组记录
# ---------------------------------------------------------------------------

def submit_group(
    watermark_id: int,
    *,
    scopes: list[str] | None = None,
    request_id: str | None = None,
    operator: str = "责任审核人",
    inject_failure: bool = False,
) -> dict[str, Any]:
    """同一请求同时提交两种口径：整组只落一次，失败整组回退。

    返回 `accepted` 表示整组结果被接收；重复提交（同 request_id）返回同一组版本。
    """
    requested = scopes or list(SCOPES)
    invalid = [scope for scope in requested if scope not in SCOPES]
    if invalid:
        raise LateBypassError("invalid_scope", f"未知口径：{'、'.join(invalid)}")

    with _LOCK:
        _find_watermark(watermark_id)
        group_key = request_id or _stable_hash("group", watermark_id, *requested)
        existing_submission = _submission_by_key(group_key)
        if existing_submission is not None:
            return {
                "ok": True,
                "accepted": True,
                "created": False,
                "idempotent": True,
                "reason": "同一整组提交已落过记录",
                "group_key": group_key,
                "submission": _public(existing_submission),
                "versions": [
                    _version_public(row)
                    for row in store.rows("late_bypass")
                    if int(row.get("id", 0)) in set(existing_submission.get("version_ids", []))
                ],
            }

        snapshot = _snapshot()
        try:
            versions = []
            for scope in requested:
                result = freeze_version(
                    watermark_id,
                    scope,
                    request_id=request_id,
                    operator=operator,
                )
                version = result.get("version")
                if version:
                    row = store.find("late_bypass", int(version["id"]))
                    if row is not None:
                        row["_group_key"] = group_key
                        versions.append(row)
            if inject_failure:
                raise LateBypassError("transaction_failed", "整组提交事务失败")
            submission = {
                "id": _next_id("late_submission"),
                "group_key": group_key,
                "request_id": request_id or "",
                "watermark_id": watermark_id,
                "scopes": ",".join(requested),
                "version_ids": [int(row.get("id", 0)) for row in versions],
                "status": "已接收",
                "pending": False,
                "abnormal": False,
                "created_at": time.time(),
            }
            store.rows("late_submission").append(submission)
            return {
                "ok": True,
                "accepted": True,
                "created": bool(versions),
                "idempotent": False,
                "group_key": group_key,
                "submission": _public(submission),
                "versions": [_version_public(row) for row in versions],
                "accepted_scopes": [row.get("口径") for row in versions],
                "rejected_scopes": [
                    scope for scope in requested
                    if scope not in {row.get("口径") for row in versions}
                ],
            }
        except LateBypassError:
            _restore(snapshot)
            raise
        except Exception as exc:  # pragma: no cover - 兜底
            _restore(snapshot)
            raise LateBypassError("transaction_failed", f"整组提交失败：{exc}") from exc


def _submission_by_key(group_key: str) -> dict[str, Any] | None:
    for row in store.rows("late_submission"):
        if row.get("group_key") == group_key:
            return row
    return None


def _public(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if not key.startswith("_")}


# ---------------------------------------------------------------------------
# 水位页面 / 异常来源关系
# ---------------------------------------------------------------------------

def watermark_overview() -> list[dict[str, Any]]:
    """水位页面：水位记录 + 迟到分布摘要 + 旁路版本/补数作业来源关系。"""
    rows = []
    for record in store.rows("watermark_track"):
        watermark_id = int(record.get("id", 0))
        current = analyse_late(record, "current")
        lookback = analyse_late(record, "lookback")
        versions = [
            row for row in store.rows("late_bypass")
            if int(row.get("水位记录ID", 0)) == watermark_id
        ]
        jobs = []
        receipts = []
        for version in versions:
            job = _job_of_version(version)
            if job is not None:
                jobs.append({
                    "作业ID": job.get("id"),
                    "作业名称": job.get("作业名称", ""),
                    "作业状态": job.get("status", ""),
                    "补数范围": job.get("补数范围", ""),
                    "覆盖事件范围": job.get("_covered_event_range", ""),
                    "版本号": version.get("版本号", ""),
                })
                receipts.extend(_receipt_of_version(int(version.get("id", 0))))
        rows.append({
            "id": watermark_id,
            "水位名称": record.get("水位名称", ""),
            "数据源": record.get("数据源", ""),
            "当前水位": int(_to_float(record.get("当前水位"))),
            "水位区间": current["interval_label"],
            "上水位": int(current["interval"]["previous"]),
            "延迟阈值": current["interval"]["allowed"],
            "迟到截止": int(current["interval"]["cutoff"]),
            "当前口径迟到数": current["late_count"],
            "回看口径迟到数": lookback["late_count"],
            "迟到率": current["late_ratio"],
            "阈值百分比": _round(current["threshold"] * 100, 2),
            "阈值命中": current["threshold_hit"],
            "重复到达数": current["duplicate_count"],
            "已旁路数": current["already_bypassed_count"],
            "待旁路数": current["candidate_count"],
            "旁路版本数": len(versions),
            "旁路版本": [version.get("版本号", "") for version in versions],
            "补数作业数": len(jobs),
            "补数作业": jobs,
            "回执数": len(receipts),
            "来源关系": [
                {
                    "水位记录ID": watermark_id,
                    "旁路版本ID": version.get("id"),
                    "版本号": version.get("版本号", ""),
                    "口径": version.get("口径名称", ""),
                    "作业ID": (_job_of_version(version) or {}).get("id"),
                    "回执数": len(_receipt_of_version(int(version.get("id", 0)))),
                }
                for version in versions
            ],
            "status": record.get("status", ""),
            "pending": bool(current["candidate_count"]) or record.get("pending", False),
            "abnormal": bool(record.get("abnormal", False)) or current["threshold_hit"],
        })
    return rows


def void_version(version_id: int, *, request_id: str | None = None) -> dict[str, Any]:
    """作废候选版本：不删除旁路事件归属，避免同一事件再进第二个版本。"""
    with _LOCK:
        version = store.find("late_bypass", version_id)
        if version is None:
            raise LateBypassError("version_not_found", f"旁路版本 {version_id} 不存在")
        if version.get("_job_id") is not None and version.get("status") != "部分失败":
            raise LateBypassError("version_in_use", "版本已生成补数作业，不能直接作废")
        version["status"] = "已作废"
        version["pending"] = False
        version["abnormal"] = False
        version["_void_request"] = request_id or ""
        # 释放事件归属：同一事件在任一时刻只属于一个版本，作废后可进入新版本。
        store._tables["late_bypass_event"] = [
            row for row in store.rows("late_bypass_event")
            if int(row.get("bypass_id", 0)) != version_id
        ]
        for row in store.rows("late_bypass_current"):
            if int(row.get("当前版本ID", 0)) == version_id:
                row["status"] = "候选结果"
                row["可执行"] = False
                row["pending"] = True
        _sync_watermark_record(int(version.get("水位记录ID", 0)))
        _close_todos("watermark_track", version_id=version_id)
        return {"ok": True, "version": _version_public(version)}
