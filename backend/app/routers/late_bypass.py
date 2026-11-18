"""迟到旁路链路接口：水位详情、迟到分布、旁路版本、补数作业与阈值核对。

与通用资源路由分开注册，避免 /api/{module}/{entry_id} 抢先匹配固定路径。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import late_bypass as svc

router = APIRouter(prefix="/api/late-bypass", tags=["迟到旁路"])


class FreezePayload(BaseModel):
    watermark_id: int
    scope: str = "current"
    request_id: str | None = None
    operator: str = "责任审核人"
    inject_failure: bool = False


class GroupSubmitPayload(BaseModel):
    watermark_id: int
    scopes: list[str] = Field(default_factory=lambda: list(svc.SCOPES))
    request_id: str | None = None
    operator: str = "责任审核人"
    inject_failure: bool = False


class ConfirmPayload(BaseModel):
    request_id: str | None = None
    operator: str = "责任审核人"
    inject_failure: bool = False


class ExecutePayload(BaseModel):
    request_id: str | None = None
    operator: str = "责任审核人"
    inject_failure: bool = False


class ReopenPayload(BaseModel):
    request_id: str | None = None
    operator: str = "责任审核人"


class VoidPayload(BaseModel):
    request_id: str | None = None


def _fail(error: svc.LateBypassError) -> HTTPException:
    status = 404 if error.code.endswith("_not_found") else 409
    if error.code in ("invalid_scope",):
        status = 400
    if error.code == "transaction_failed":
        status = 500
    return HTTPException(status_code=status, detail={"code": error.code, "message": error.message})


# ---- 水位页面 ----

@router.get("/watermarks")
def list_watermarks() -> dict[str, Any]:
    rows = svc.watermark_overview()
    return {"items": rows, "total": len(rows), "summary": svc.summary()}


@router.get("/watermarks/{watermark_id}/distribution")
def watermark_distribution(watermark_id: int) -> dict[str, Any]:
    try:
        return svc.late_distribution(watermark_id)
    except svc.LateBypassError as error:
        raise _fail(error) from error


# ---- 旁路版本 ----

@router.get("/versions")
def list_versions(
    source: str | None = Query(default=None),
    watermark_id: int | None = Query(default=None),
    status: str | None = Query(default=None),
    scope: str | None = Query(default=None),
) -> dict[str, Any]:
    rows = svc.list_versions(
        source=source, watermark_id=watermark_id, status=status, scope=scope,
    )
    return {"items": rows, "total": len(rows)}


@router.post("/versions")
def freeze_version(payload: FreezePayload) -> dict[str, Any]:
    try:
        return svc.freeze_version(
            payload.watermark_id,
            payload.scope,
            request_id=payload.request_id,
            operator=payload.operator,
            inject_failure=payload.inject_failure,
        )
    except svc.LateBypassError as error:
        raise _fail(error) from error


@router.post("/versions/submit-group")
def submit_group(payload: GroupSubmitPayload) -> dict[str, Any]:
    try:
        return svc.submit_group(
            payload.watermark_id,
            scopes=payload.scopes,
            request_id=payload.request_id,
            operator=payload.operator,
            inject_failure=payload.inject_failure,
        )
    except svc.LateBypassError as error:
        raise _fail(error) from error


@router.get("/versions/by-request/{request_id}")
def versions_by_request(request_id: str) -> dict[str, Any]:
    rows = [
        svc._version_public(row) for row in svc.store.rows("late_bypass")
        if row.get("_request_id") == request_id
    ]
    return {"items": rows, "total": len(rows)}


@router.get("/versions/{version_id}")
def version_detail(version_id: int) -> dict[str, Any]:
    try:
        return svc.version_detail(version_id)
    except svc.LateBypassError as error:
        raise _fail(error) from error


@router.post("/versions/{version_id}/confirm")
def confirm_version(version_id: int, payload: ConfirmPayload) -> dict[str, Any]:
    try:
        return svc.confirm_version(
            version_id,
            request_id=payload.request_id,
            operator=payload.operator,
            inject_failure=payload.inject_failure,
        )
    except svc.LateBypassError as error:
        raise _fail(error) from error


@router.post("/versions/{version_id}/reopen")
def reopen_version(version_id: int, payload: ReopenPayload) -> dict[str, Any]:
    try:
        return svc.reopen_version(version_id, request_id=payload.request_id)
    except svc.LateBypassError as error:
        raise _fail(error) from error


@router.post("/versions/{version_id}/void")
def void_version(version_id: int, payload: VoidPayload) -> dict[str, Any]:
    try:
        return svc.void_version(version_id, request_id=payload.request_id)
    except svc.LateBypassError as error:
        raise _fail(error) from error


# ---- 补数任务 / 回执 ----

@router.get("/jobs")
def list_jobs(source: str | None = Query(default=None)) -> dict[str, Any]:
    rows = svc.list_jobs(source=source)
    return {"items": rows, "total": len(rows), "summary": svc.summary()}


@router.post("/jobs/{job_id}/execute")
def execute_job(job_id: int, payload: ExecutePayload) -> dict[str, Any]:
    try:
        return svc.execute_job(
            job_id,
            request_id=payload.request_id,
            operator=payload.operator,
            inject_failure=payload.inject_failure,
        )
    except svc.LateBypassError as error:
        raise _fail(error) from error


@router.get("/receipts")
def list_receipts(
    version_id: int | None = Query(default=None),
    job_id: int | None = Query(default=None),
) -> dict[str, Any]:
    rows = svc.list_receipts(version_id=version_id, job_id=job_id)
    return {"items": rows, "total": len(rows)}


# ---- 阈值核对 ----

@router.get("/audit")
def audit(
    watermark_id: int | None = Query(default=None),
    start: float | None = Query(default=None),
    end: float | None = Query(default=None),
) -> dict[str, Any]:
    return svc.audit(watermark_id=watermark_id, start=start, end=end)


@router.get("/todos")
def list_todos(module: str | None = Query(default=None)) -> dict[str, Any]:
    rows = svc.list_todos(module=module)
    return {"items": rows, "total": len(rows)}


@router.get("/summary")
def summary() -> dict[str, Any]:
    return svc.summary()
