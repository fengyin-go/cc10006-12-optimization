"""水位治理接口：事件接入、隔离清单、回算/中断、检查点、对账与演示场景。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.watermark import WatermarkError, watermark_service

router = APIRouter(prefix="/api/watermark", tags=["水位治理"])


class IngestPayload(BaseModel):
    event_id: str
    event_time: str = Field(description="YYYY-MM-DD HH:MM 或 ISO 时间")
    source_id: str
    value: int = 0
    fail_times: int = 0


class RecomputePayload(BaseModel):
    quarantine_id: str


class AbortPayload(BaseModel):
    quarantine_id: str
    reason: str = "人工中断"


class CheckpointPayload(BaseModel):
    label: str = ""


class RestorePayload(BaseModel):
    checkpoint_id: str


class TickPayload(BaseModel):
    event_time: str


def _error(exc: WatermarkError) -> dict[str, Any]:
    return {"ok": False, "reason": exc.reason, "message": exc.message}


@router.get("/state")
def get_state() -> dict[str, Any]:
    """水位、窗口、隔离清单、检查点、质量台账与对账结论一次性返回。"""
    return {"ok": True, "data": watermark_service.state()}


@router.post("/ingest")
def ingest_event(payload: IngestPayload) -> dict[str, Any]:
    """接入事件：超窗进隔离清单，重复事件只登记来源标识。"""
    try:
        result = watermark_service.ingest(
            payload.event_id,
            payload.event_time,
            payload.source_id,
            payload.value,
            fail_times=payload.fail_times,
        )
    except WatermarkError as exc:
        return _error(exc)
    return {"ok": True, "data": result}


@router.post("/tick")
def tick_watermark(payload: TickPayload) -> dict[str, Any]:
    """不接入事件只推进水位，用于验证水位推进前后归属稳定。"""
    try:
        result = watermark_service.tick(payload.event_time)
    except WatermarkError as exc:
        return _error(exc)
    return {"ok": True, "data": result}


@router.post("/settle")
def settle_windows() -> dict[str, Any]:
    """按水位边界结算到期窗口，已结算/已修补窗口不会被旧结果覆盖。"""
    settled = watermark_service.settle_due()
    return {"ok": True, "data": settled}


@router.post("/settle-batch")
def settle_batch() -> dict[str, Any]:
    """正常窗口与超窗窗口同时结算，分别标注水位边界与隔离回算边界。"""
    return {"ok": True, "data": watermark_service.settle_batch()}


@router.post("/quarantine/recompute")
def recompute_quarantine(payload: RecomputePayload) -> dict[str, Any]:
    """隔离事件人工恢复/策略回算；失败按原因登记，可重试，重复回算被拦截。"""
    try:
        window = watermark_service.recompute(payload.quarantine_id)
    except WatermarkError as exc:
        return _error(exc)
    return {"ok": True, "data": window}


@router.post("/quarantine/abort")
def abort_quarantine(payload: AbortPayload) -> dict[str, Any]:
    """中断处理按原因登记，后续仍可重试恢复。"""
    try:
        record = watermark_service.abort(payload.quarantine_id, payload.reason)
    except WatermarkError as exc:
        return _error(exc)
    return {"ok": True, "data": record}


@router.post("/checkpoint")
def save_checkpoint(payload: CheckpointPayload) -> dict[str, Any]:
    """保存全量检查点。"""
    return {"ok": True, "data": watermark_service.checkpoint(payload.label)}


@router.post("/restore")
def restore_checkpoint(payload: RestorePayload) -> dict[str, Any]:
    """从检查点恢复，已修补窗口保持修补后的结果。"""
    try:
        result = watermark_service.restore(payload.checkpoint_id)
    except WatermarkError as exc:
        return _error(exc)
    return {"ok": True, "data": result}


@router.get("/reconcile")
def reconcile() -> dict[str, Any]:
    """对账：隔离区与正式窗口条数之和等于输入去重事件数，超窗记录不静默消失。"""
    return {"ok": True, "data": watermark_service.reconcile()}


@router.post("/demo/run")
def run_demo() -> dict[str, Any]:
    """回放题目要求的完整场景：12分钟迟到×3、回算、检查点恢复、跨零点与对账。"""
    return {"ok": True, "data": watermark_service.run_demo_scenario()}


@router.post("/reset")
def reset_engine() -> dict[str, Any]:
    """清空引擎状态并重新同步质量台账与流处理作业。"""
    watermark_service.reset()
    return {"ok": True, "data": watermark_service.state()}
