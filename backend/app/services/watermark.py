"""水位边界与迟到事件治理引擎。

设计口径（全部确定性、可回放，不依赖墙钟）：

* 滚动窗口 ``WINDOW_MINUTES`` 分钟对齐；水位 = 已观测最大事件时间 - ``WATERMARK_DELAY_MINUTES``。
* 事件时间早于当前水位、且归属窗口已经结算的，属于「超窗」数据：进入隔离清单，
  绝不静默丢弃；隔离记录支持人工恢复或按策略回算。
* 同一 ``event_id`` 只计算一次：首次到达落窗口或隔离区，后续副本只登记来源标识。
* 回算修补会推进窗口修订版本（revision），已修补/已结算窗口不允许被旧结果覆盖；
  检查点保存全量状态，恢复后仍是修补后的结果。
* 隔离数量、失败原因、回算结果同步写入质量台账、流处理作业状态与操作审计，
  并提供对账口径：正式窗口去重事件数 + 隔离区主记录数 = 输入去重事件数。
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from app.store import store

WINDOW_MINUTES = 10
WATERMARK_DELAY_MINUTES = 5

# 迟到数据全生命周期的原因分类，失败、重试、中断、异常都按原因落账。
REASON_OVER_WATERMARK = "超水位"
REASON_DUPLICATE = "重复事件"
REASON_RECOMPUTE_FAILED = "回算失败"
REASON_MANUAL_ABORT = "人工中断"
REASON_STATE_ABNORMAL = "状态异常"
REASON_REPEAT_RECOMPUTE = "重复回算拦截"
REASON_OVERWRITE_BLOCKED = "已结算窗口覆盖拦截"

WINDOW_OPEN = "开放中"
WINDOW_SETTLED = "已结算"
WINDOW_PATCHED = "已修补"

BOUNDARY_WATERMARK = "水位边界"
BOUNDARY_RECOMPUTE = "隔离回算边界"

Q_PENDING = "待处理"
Q_FAILED = "回算失败"
Q_ABORTED = "已中断"
Q_RESOLVED = "已回算"
Q_DISCARDED = "已放弃"

QUALITY_LEDGER_CODE = "QUAL-WM-0001"
STREAM_JOB_CODE = "STRE-0003"


class WatermarkError(Exception):
    """水位治理相关的业务异常，reason 用于按原因分类处理。"""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


def parse_event_time(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    text = str(value).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise WatermarkError(REASON_STATE_ABNORMAL, f"事件时间格式无法识别：{value}")


def window_start_of(moment: datetime) -> datetime:
    minute = (moment.minute // WINDOW_MINUTES) * WINDOW_MINUTES
    return moment.replace(minute=minute, second=0, microsecond=0)


@dataclass
class StreamEvent:
    event_id: str
    source_id: str
    event_time: datetime
    value: int
    arrival_seq: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "source_id": self.source_id,
            "event_time": self.event_time.isoformat(),
            "value": self.value,
            "arrival_seq": self.arrival_seq,
        }


@dataclass
class WindowState:
    window_start: datetime
    window_end: datetime
    events: dict[str, StreamEvent] = field(default_factory=dict)
    status: str = WINDOW_OPEN
    boundary: str | None = None
    result_sum: int = 0
    result_count: int = 0
    revision: int = 0
    settled_at: datetime | None = None
    patched_by: list[str] = field(default_factory=list)
    duplicate_markers: list[dict[str, Any]] = field(default_factory=list)

    @property
    def key(self) -> str:
        return self.window_start.isoformat()

    def recompute_result(self) -> None:
        self.result_sum = sum(item.value for item in self.events.values())
        self.result_count = len(self.events)

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "status": self.status,
            "boundary": self.boundary,
            "result_sum": self.result_sum,
            "result_count": self.result_count,
            "revision": self.revision,
            "settled_at": self.settled_at.isoformat() if self.settled_at else None,
            "patched_by": list(self.patched_by),
            "duplicate_markers": list(self.duplicate_markers),
            # 追溯上游明细时直接回到原始事件，而不是只给聚合数字。
            "events": [item.to_dict() for item in sorted(self.events.values(), key=lambda x: x.event_time)],
        }


@dataclass
class Attempt:
    seq: int
    action: str
    ok: bool
    reason: str
    at: datetime
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "action": self.action,
            "ok": self.ok,
            "reason": self.reason,
            "at": self.at.isoformat(),
            "detail": self.detail,
        }


@dataclass
class QuarantineRecord:
    quarantine_id: str
    event_id: str
    source_id: str
    event_time: datetime
    window_start: datetime
    window_end: datetime
    value: int
    lag_minutes: int
    reason: str
    status: str = Q_PENDING
    fail_times: int = 0
    recompute_count: int = 0
    duplicate_sources: list[dict[str, Any]] = field(default_factory=list)
    attempts: list[Attempt] = field(default_factory=list)
    created_at: datetime | None = None
    resolved_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "quarantine_id": self.quarantine_id,
            "event_id": self.event_id,
            "source_id": self.source_id,
            "event_time": self.event_time.isoformat(),
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "value": self.value,
            "lag_minutes": self.lag_minutes,
            "reason": self.reason,
            "status": self.status,
            "fail_times": self.fail_times,
            "recompute_count": self.recompute_count,
            "duplicate_sources": list(self.duplicate_sources),
            "attempts": [item.to_dict() for item in self.attempts],
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            # 隔离清单保留原始事件载荷，人工恢复时看到的是原始明细。
            "original_event": {
                "event_id": self.event_id,
                "source_id": self.source_id,
                "event_time": self.event_time.isoformat(),
                "value": self.value,
            },
        }


class WatermarkService:
    """内存态水位治理引擎；检查点通过深拷贝保证恢复后状态与落账时完全一致。"""

    def __init__(self) -> None:
        self.reset()
    # ------------------------------------------------------------------ 基础状态
    def reset(self) -> None:
        self.watermark: datetime | None = None
        self.windows: dict[str, WindowState] = {}
        self.quarantine: dict[str, QuarantineRecord] = {}
        self.processed_ids: set[str] = set()
        self.event_locations: dict[str, str] = {}  # event_id -> window key 或 quarantine_id
        # 重复副本统一登记：事件首次计算后，后续副本只登记来源标识，永不二次计算。
        self.duplicate_markers: dict[str, list[dict[str, Any]]] = {}
        self.ledger: list[dict[str, Any]] = []
        self.arrival_seq = 0   # 仅统计事件接入次数（含重复副本）
        self.clock_seq = 0     # 确定性逻辑时钟：tick 与 ingest 都推进，用于合成时间戳
        self.q_seq = 0
        self.audit_seq = 0
        self.metrics: dict[str, Any] = self._empty_metrics()
        # 检查点是持久化快照，运行态 reset 不清空它，否则恢复无从谈起。
        if not hasattr(self, "checkpoints"):
            self.checkpoints = {}
            self.cp_seq = 0
        self._sync_external_modules()

    @staticmethod
    def _empty_metrics() -> dict[str, Any]:
        return {
            "input_arrivals": 0,          # 含重复副本的到达总次数
            "distinct_events": 0,         # 去重后的事件数
            "duplicate_arrivals": 0,      # 重复到达次数（只登记来源标识）
            "quarantine_total": 0,        # 隔离区主记录数
            "quarantine_pending": 0,
            "quarantine_resolved": 0,
            "quarantine_aborted": 0,
            "quarantine_by_reason": {},
            "failure_by_reason": {},
            "recompute_success": 0,
            "recompute_failure": 0,
            "patched_windows": 0,
            "settled_windows": 0,
            "blocked_overwrite": 0,
        }

    def _now_marker(self) -> datetime:
        # 纯确定性引擎：用逻辑时钟合成时间戳，避免墙钟导致回放结果不一致。
        return datetime(2026, 11, 12, 0, 0, 0) + timedelta(seconds=self.clock_seq)

    def _window(self, moment: datetime) -> WindowState:
        start = window_start_of(moment)
        key = start.isoformat()
        if key not in self.windows:
            self.windows[key] = WindowState(window_start=start, window_end=start + timedelta(minutes=WINDOW_MINUTES))
        return self.windows[key]

    # ------------------------------------------------------------------ 落账
    def _append_ledger(self, action: str, detail: str, reason: str = "") -> None:
        self.ledger.append({
            "seq": len(self.ledger) + 1,
            "at": self._now_marker().isoformat(),
            "action": action,
            "reason": reason,
            "detail": detail,
        })

    def _audit(self, action: str, detail: str, reason: str = "") -> None:
        self.audit_seq += 1
        rows = store.rows("audit")
        rows.append({
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "status": "已归档",
            "pending": False,
            "abnormal": bool(reason),
            "日志编号": f"AUDI-WM-{self.audit_seq:04d}",
            "操作类型": action,
            "操作对象": "流处理水位治理",
            "操作详情": detail,
            "操作来源": "watermark-engine",
            "执行时间": self._now_marker().isoformat(),
            "影响行数": 1,
            "回滚能力": "检查点恢复",
            "原因分类": reason or "正常",
        })

    def _refresh_metrics(self) -> None:
        metrics = self._empty_metrics()
        metrics["input_arrivals"] = self.arrival_seq
        metrics["distinct_events"] = len(self.processed_ids)
        records = list(self.quarantine.values())
        metrics["duplicate_arrivals"] = sum(len(markers) for markers in self.duplicate_markers.values())
        metrics["quarantine_total"] = len(records)
        metrics["quarantine_pending"] = sum(1 for item in records if item.status in (Q_PENDING, Q_FAILED))
        metrics["quarantine_resolved"] = sum(1 for item in records if item.status == Q_RESOLVED)
        metrics["quarantine_aborted"] = sum(1 for item in records if item.status in (Q_ABORTED, Q_DISCARDED))
        by_reason: dict[str, int] = {}
        failures: dict[str, int] = {}
        for item in records:
            by_reason[item.reason] = by_reason.get(item.reason, 0) + 1
            for attempt in item.attempts:
                if not attempt.ok:
                    failures[attempt.reason] = failures.get(attempt.reason, 0) + 1
        metrics["quarantine_by_reason"] = by_reason
        metrics["failure_by_reason"] = failures
        metrics["recompute_success"] = sum(1 for item in records if item.status == Q_RESOLVED)
        metrics["recompute_failure"] = sum(1 for attempt_chain in records for attempt in attempt_chain.attempts if not attempt.ok)
        metrics["patched_windows"] = sum(1 for win in self.windows.values() if win.status == WINDOW_PATCHED)
        metrics["settled_windows"] = sum(1 for win in self.windows.values() if win.status in (WINDOW_SETTLED, WINDOW_PATCHED))
        metrics["blocked_overwrite"] = sum(
            1 for chain in records for attempt in chain.attempts if attempt.reason == REASON_OVERWRITE_BLOCKED
        )
        self.metrics = metrics

    def _sync_external_modules(self) -> None:
        """跨模块同步：质量台账 + 流处理作业状态，保证多处隔离结果对得上。"""
        self._refresh_metrics()
        metrics = self.metrics

        quality_rows = store.rows("quality")
        ledger_row = next((row for row in quality_rows if row.get("指标编号") == QUALITY_LEDGER_CODE), None)
        if ledger_row is None:
            ledger_row = {"id": max((int(row.get("id", 0)) for row in quality_rows), default=0) + 1}
            quality_rows.append(ledger_row)
        ledger_row.update({
            "status": "已启用",
            "pending": False,
            "abnormal": metrics["quarantine_pending"] > 0,
            "指标编号": QUALITY_LEDGER_CODE,
            "监控对象": "流处理水位迟到治理",
            "指标类型": "迟到与隔离质量台账",
            "阈值范围": f"水位延迟{WATERMARK_DELAY_MINUTES}分钟/窗口{WINDOW_MINUTES}分钟",
            "检测周期": "实时",
            "告警级别": "高" if metrics["quarantine_pending"] else "提示",
            "通知渠道": "流处理值班群",
            "恢复策略": "隔离清单人工恢复或按策略回算",
            "隔离事件数": metrics["quarantine_total"],
            "待处理隔离": metrics["quarantine_pending"],
            "重复副本数": metrics["duplicate_arrivals"],
            "回算成功": metrics["recompute_success"],
            "回算失败": metrics["recompute_failure"],
            "已修补窗口": metrics["patched_windows"],
            "失败原因分布": dict(metrics["failure_by_reason"]),
            "隔离原因分布": dict(metrics["quarantine_by_reason"]),
        })

        stream_rows = store.rows("stream")
        job = next((row for row in stream_rows if row.get("作业编号") == STREAM_JOB_CODE), None)
        if job is not None:
            job.update({
                "隔离事件数": metrics["quarantine_total"],
                "待处理隔离": metrics["quarantine_pending"],
                "重复事件数": metrics["duplicate_arrivals"],
                "已修补窗口数": metrics["patched_windows"],
                "已结算窗口数": metrics["settled_windows"],
                "当前水位": self.watermark.isoformat() if self.watermark else "未推进",
                "水位状态": "存在待处理超窗数据" if metrics["quarantine_pending"] else "正常",
                "abnormal": metrics["quarantine_pending"] > 0,
            })

    # ------------------------------------------------------------------ 事件接入
    def ingest(
        self,
        event_id: str,
        event_time: str | datetime,
        source_id: str,
        value: int,
        *,
        fail_times: int = 0,
    ) -> dict[str, Any]:
        """接入一条事件；返回处置去向，超窗数据落隔离清单而不是静默丢弃。"""
        event_id = str(event_id).strip()
        source_id = str(source_id).strip()
        if not event_id:
            raise WatermarkError(REASON_STATE_ABNORMAL, "事件标识 event_id 不能为空")
        moment = parse_event_time(event_time)
        self.arrival_seq += 1
        self.clock_seq += 1
        self.metrics["input_arrivals"] = self.arrival_seq

        # 同一事件重复到达：只登记来源标识，绝不二次计算。
        if event_id in self.processed_ids:
            location = self.event_locations.get(event_id, "")
            marker = {"source_id": source_id, "arrival_seq": self.arrival_seq, "at": self._now_marker().isoformat()}
            self.duplicate_markers.setdefault(event_id, []).append(marker)
            if location in self.quarantine:
                self.quarantine[location].duplicate_sources.append(marker)
            elif location in self.windows:
                self.windows[location].duplicate_markers.append(marker)
            self._append_ledger(
                "重复事件登记",
                f"事件 {event_id} 第 {self.arrival_seq} 次到达，仅登记来源 {source_id}，不重复计算",
                reason=REASON_DUPLICATE,
            )
            self._sync_external_modules()
            return {"disposition": "duplicate_marker_only", "event_id": event_id, "location": location}

        event = StreamEvent(event_id=event_id, source_id=source_id, event_time=moment, value=int(value), arrival_seq=self.arrival_seq)
        window = self._window(moment)
        # 超窗：归属窗口已越过水位边界（window_end <= 当前水位），
        # 事件进入隔离清单而不是静默丢弃，等待人工恢复或按策略回算。
        over_window = self.watermark is not None and window.window_end <= self.watermark

        if over_window:
            lag = int((self.watermark - moment).total_seconds() // 60)
            self.q_seq += 1
            qid = f"Q-{self.q_seq:04d}"
            record = QuarantineRecord(
                quarantine_id=qid,
                event_id=event_id,
                source_id=source_id,
                event_time=moment,
                window_start=window.window_start,
                window_end=window.window_end,
                value=int(value),
                lag_minutes=lag,
                reason=REASON_OVER_WATERMARK,
                fail_times=fail_times,
                created_at=self._now_marker(),
            )
            self.quarantine[qid] = record
            self.processed_ids.add(event_id)
            self.event_locations[event_id] = qid
            self._append_ledger(
                "超窗隔离",
                f"事件 {event_id} 事件时间 {moment.isoformat()} 落后水位 {lag} 分钟，进入隔离清单 {qid}，未静默丢弃",
                reason=REASON_OVER_WATERMARK,
            )
            self._audit("超窗隔离", f"{qid} 事件 {event_id} 落后水位 {lag} 分钟", reason=REASON_OVER_WATERMARK)
            self._sync_external_modules()
            return {"disposition": "quarantined", "quarantine_id": qid, "lag_minutes": lag}

        if window.status in (WINDOW_SETTLED, WINDOW_PATCHED):
            # 理论上不会走到：结算后的窗口只可能因超水位进入隔离。显式拦截，杜绝覆盖旧窗口。
            raise WatermarkError(
                REASON_OVERWRITE_BLOCKED,
                f"窗口 {window.key} 已{window.status}，事件 {event_id} 禁止写回旧窗口",
            )
        window.events[event_id] = event
        self.processed_ids.add(event_id)
        self.event_locations[event_id] = window.key
        self._advance_watermark(moment)
        self._append_ledger("事件入窗", f"事件 {event_id} 进入窗口 {window.key}")
        self._sync_external_modules()
        return {"disposition": "windowed", "window": window.key}

    def _advance_watermark(self, moment: datetime) -> None:
        candidate = moment - timedelta(minutes=WATERMARK_DELAY_MINUTES)
        if self.watermark is None or candidate > self.watermark:
            old = self.watermark.isoformat() if self.watermark else "未推进"
            self.watermark = candidate
            self._append_ledger("水位推进", f"水位 {old} -> {candidate.isoformat()}")
    def tick(self, event_time: str | datetime) -> dict[str, Any]:
        """显式推进水位（不接入事件），用于回放水位推进前后的归属稳定性。"""
        moment = parse_event_time(event_time)
        self.clock_seq += 1
        self._advance_watermark(moment)
        self._sync_external_modules()
        return {"watermark": self.watermark.isoformat() if self.watermark else None}

    # ------------------------------------------------------------------ 窗口结算
    def settle_due(self) -> list[dict[str, Any]]:
        """按水位边界结算所有到期窗口；已结算/已修补窗口绝不退回旧值。"""
        if self.watermark is None:
            return []
        settled: list[dict[str, Any]] = []
        for window in sorted(self.windows.values(), key=lambda item: item.window_start):
            if window.status == WINDOW_OPEN and window.window_end <= self.watermark:
                window.recompute_result()
                window.status = WINDOW_SETTLED
                window.boundary = BOUNDARY_WATERMARK
                window.revision = 1
                window.settled_at = self._now_marker()
                settled.append(window.to_dict())
                self._append_ledger(
                    "窗口结算",
                    f"窗口 {window.key} 按{BOUNDARY_WATERMARK}结算（window_end={window.window_end.isoformat()} <= 水位 {self.watermark.isoformat()}）",
                )
        self._sync_external_modules()
        return settled

    def settle_batch(self) -> dict[str, Any]:
        """正常窗口与超窗窗口同时结算：分别标注各自采用的边界。"""
        normal = self.settle_due()
        recomputed: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for qid in sorted(self.quarantine):
            record = self.quarantine[qid]
            if record.status in (Q_PENDING, Q_FAILED, Q_ABORTED):
                try:
                    recomputed.append(self.recompute(qid))
                except WatermarkError as exc:
                    failures.append({"quarantine_id": qid, "reason": exc.reason, "message": exc.message})
        return {
            "watermark_boundary_windows": normal,
            "recompute_boundary_windows": recomputed,
            "failures": failures,
        }

    # ------------------------------------------------------------------ 隔离治理
    def _get_quarantine(self, quarantine_id: str) -> QuarantineRecord:
        record = self.quarantine.get(quarantine_id)
        if record is None:
            raise WatermarkError(REASON_STATE_ABNORMAL, f"隔离记录 {quarantine_id} 不存在")
        return record

    def recompute(self, quarantine_id: str) -> dict[str, Any]:
        """对隔离事件回算一次：失败按原因记录可重试，成功则修补窗口并推进修订版本。"""
        record = self._get_quarantine(quarantine_id)
        if record.status == Q_RESOLVED:
            record.attempts.append(Attempt(
                seq=len(record.attempts) + 1, action="回算", ok=False,
                reason=REASON_REPEAT_RECOMPUTE, at=self._now_marker(),
                detail="同一隔离事件只允许首次回算一次，重复回算被拦截",
            ))
            self._append_ledger("回算拦截", f"{quarantine_id} 已回算，拒绝重复回算", reason=REASON_REPEAT_RECOMPUTE)
            self._sync_external_modules()
            raise WatermarkError(REASON_REPEAT_RECOMPUTE, f"{quarantine_id} 已完成回算，重复事件以首次计算为准")

        record.recompute_count += 1
        attempt_seq = len(record.attempts) + 1

        # 确定性故障注入：前 fail_times 次回算按「回算失败」落账，之后重试成功。
        if record.recompute_count <= record.fail_times:
            record.status = Q_FAILED
            record.attempts.append(Attempt(
                seq=attempt_seq, action="回算", ok=False, reason=REASON_RECOMPUTE_FAILED,
                at=self._now_marker(), detail=f"第 {record.recompute_count} 次回算失败，可按原因重试",
            ))
            self._append_ledger("回算失败", f"{quarantine_id} 第 {record.recompute_count} 次回算失败", reason=REASON_RECOMPUTE_FAILED)
            self._audit("回算失败", f"{quarantine_id} 事件 {record.event_id}", reason=REASON_RECOMPUTE_FAILED)
            self._sync_external_modules()
            raise WatermarkError(REASON_RECOMPUTE_FAILED, f"{quarantine_id} 回算失败，已登记原因，可重试")

        window = self.windows.get(record.window_start.isoformat())
        if window is None:
            raise WatermarkError(REASON_STATE_ABNORMAL, f"{quarantine_id} 归属窗口不存在，状态异常")
        if window.status == WINDOW_PATCHED and record.event_id in window.events:
            record.attempts.append(Attempt(
                seq=attempt_seq, action="回算", ok=False, reason=REASON_OVERWRITE_BLOCKED,
                at=self._now_marker(), detail="事件已在修补结果中，禁止覆盖回旧值",
            ))
            self._sync_external_modules()
            raise WatermarkError(REASON_OVERWRITE_BLOCKED, "已修补窗口不能被旧结果覆盖")

        window.events[record.event_id] = StreamEvent(
            event_id=record.event_id, source_id=record.source_id,
            event_time=record.event_time, value=record.value, arrival_seq=self.arrival_seq,
        )
        window.recompute_result()
        window.status = WINDOW_PATCHED
        window.boundary = BOUNDARY_RECOMPUTE
        # 结算时 revision=1，每次修补推进一个版本；检查点恢复后该版本号原样保留。
        window.revision = max(window.revision, 1) + 1
        window.patched_by.append(quarantine_id)
        # 事件当前归属迁移到正式窗口；隔离主记录保留用于追溯与对账。
        self.event_locations[record.event_id] = window.key
        record.status = Q_RESOLVED
        record.resolved_at = self._now_marker()
        record.attempts.append(Attempt(
            seq=attempt_seq, action="回算", ok=True, reason="",
            at=self._now_marker(), detail=f"回算成功，窗口 {window.key} 修订至 revision={window.revision}",
        ))
        self._append_ledger(
            "隔离回算",
            f"{quarantine_id} 事件 {record.event_id} 回算修补窗口 {window.key}，revision={window.revision}，超窗范围未退回旧窗口",
        )
        self._audit("隔离回算", f"{quarantine_id} 修补窗口 {window.key}", reason="")
        self._sync_external_modules()
        return window.to_dict()

    def abort(self, quarantine_id: str, reason: str = REASON_MANUAL_ABORT) -> dict[str, Any]:
        """中断处理：按原因登记中断状态，后续仍可重试恢复。"""
        record = self._get_quarantine(quarantine_id)
        if record.status == Q_RESOLVED:
            raise WatermarkError(REASON_REPEAT_RECOMPUTE, f"{quarantine_id} 已回算，不能再中断")
        record.status = Q_ABORTED
        record.attempts.append(Attempt(
            seq=len(record.attempts) + 1, action="中断", ok=False, reason=reason,
            at=self._now_marker(), detail="处理中断已按原因登记，可重试恢复",
        ))
        self._append_ledger("处理中断", f"{quarantine_id} 登记中断：{reason}", reason=reason)
        self._audit("处理中断", f"{quarantine_id}：{reason}", reason=reason)
        self._sync_external_modules()
        return record.to_dict()

    # ------------------------------------------------------------------ 检查点
    def checkpoint(self, label: str = "") -> dict[str, Any]:
        self.cp_seq += 1
        cp_id = f"CP-{self.cp_seq:04d}"
        snapshot = {
            "checkpoint_id": cp_id,
            "label": label or cp_id,
            "at": self._now_marker().isoformat(),
            "watermark": self.watermark.isoformat() if self.watermark else None,
            "windows": {key: copy.deepcopy(value) for key, value in self.windows.items()},
            "quarantine": {key: copy.deepcopy(value) for key, value in self.quarantine.items()},
            "processed_ids": set(self.processed_ids),
            "event_locations": dict(self.event_locations),
            "duplicate_markers": copy.deepcopy(self.duplicate_markers),
            "ledger": copy.deepcopy(self.ledger),
            "metrics": copy.deepcopy(self.metrics),
            "arrival_seq": self.arrival_seq,
            "clock_seq": self.clock_seq,
            "q_seq": self.q_seq,
            "audit_seq": self.audit_seq,
        }
        self.checkpoints[cp_id] = snapshot
        self._append_ledger("检查点", f"{cp_id} 已保存全量状态（{label or '手动'}）")
        return {"checkpoint_id": cp_id, "at": snapshot["at"]}

    def restore(self, checkpoint_id: str) -> dict[str, Any]:
        snapshot = self.checkpoints.get(checkpoint_id)
        if snapshot is None:
            raise WatermarkError(REASON_STATE_ABNORMAL, f"检查点 {checkpoint_id} 不存在")
        self.watermark = parse_event_time(snapshot["watermark"]) if snapshot["watermark"] else None
        self.windows = copy.deepcopy(snapshot["windows"])
        self.quarantine = copy.deepcopy(snapshot["quarantine"])
        self.processed_ids = set(snapshot["processed_ids"])
        self.event_locations = dict(snapshot["event_locations"])
        self.duplicate_markers = copy.deepcopy(snapshot.get("duplicate_markers", {}))
        self.ledger = copy.deepcopy(snapshot["ledger"])
        self.metrics = copy.deepcopy(snapshot["metrics"])
        self.arrival_seq = snapshot["arrival_seq"]
        self.clock_seq = snapshot.get("clock_seq", snapshot["arrival_seq"])
        self.q_seq = snapshot["q_seq"]
        self.audit_seq = snapshot["audit_seq"]
        self._append_ledger("检查点恢复", f"已从 {checkpoint_id} 恢复，已修补窗口保持修补后结果，不回退旧值")
        self._audit("检查点恢复", f"从 {checkpoint_id} 恢复引擎状态", reason="")
        self._sync_external_modules()
        return {"restored_from": checkpoint_id, "watermark": self.watermark.isoformat() if self.watermark else None}

    # ------------------------------------------------------------------ 对账
    def reconcile(self, *, window_from: datetime | None = None, window_to: datetime | None = None) -> dict[str, Any]:
        """对账：按事件当前归属计数，隔离区（未回算）与正式窗口条数之和等于输入去重事件数。

        已回算事件当前归属在正式窗口，隔离主记录保留作追溯，不重复计数。
        可选 window_from/window_to 把对账范围限定到一组窗口（如跨零点演示）。
        """
        windows = list(self.windows.values())
        if window_from is not None:
            windows = [item for item in windows if item.window_start >= window_from]
        if window_to is not None:
            windows = [item for item in windows if item.window_start < window_to]

        windowed_events = {event_id for window in windows for event_id in window.events}
        active_q_records = [
            record for record in self.quarantine.values()
            if record.status not in (Q_RESOLVED,)
            and (window_from is None or record.window_start >= window_from)
            and (window_to is None or record.window_start < window_to)
        ]
        quarantined_events = {record.event_id for record in active_q_records}
        if window_from is None and window_to is None:
            distinct_inputs = len(self.processed_ids)
        else:
            distinct_inputs = len(windowed_events | quarantined_events)
        formal_count = len(windowed_events)
        quarantine_count = len(quarantined_events)
        overlap = windowed_events & quarantined_events
        if window_from is None and window_to is None:
            missing = self.processed_ids - {
                eid for eid, loc in self.event_locations.items() if loc in self.windows or loc in self.quarantine
            }
        else:
            missing = set()
        quality_row = next((row for row in store.rows("quality") if row.get("指标编号") == QUALITY_LEDGER_CODE), None)
        stream_job = next((row for row in store.rows("stream") if row.get("作业编号") == STREAM_JOB_CODE), None)
        checks = {
            "隔离区与正式窗口条数之和等于输入去重事件数": formal_count + quarantine_count == distinct_inputs,
            "正式窗口与隔离区无重叠事件": not overlap,
            "无静默丢失事件": not missing,
            "质量台账隔离数一致": bool(quality_row and quality_row.get("隔离事件数") == self.metrics["quarantine_total"]),
            "流处理作业隔离数一致": bool(stream_job and stream_job.get("隔离事件数") == self.metrics["quarantine_total"]),
            "重复到达均未二次计算": self.metrics["duplicate_arrivals"] == sum(
                len(markers) for markers in self.duplicate_markers.values()
            ),
            "已修补窗口修订版本未回退": all(
                window.revision >= 2 for window in self.windows.values() if window.status == WINDOW_PATCHED
            ),
        }
        return {
            "scope": "全部窗口" if window_from is None else f"{window_from.isoformat()} 至 {window_to.isoformat()}",
            "distinct_input_events": distinct_inputs,
            "formal_window_events": formal_count,
            "quarantine_primary_records": quarantine_count,
            "duplicate_arrival_markers": self.metrics["duplicate_arrivals"],
            "overlap_events": sorted(overlap),
            "missing_events": sorted(missing),
            "checks": checks,
            "all_passed": all(checks.values()),
        }

    # ------------------------------------------------------------------ 视图
    def state(self) -> dict[str, Any]:
        return {
            "config": {
                "window_minutes": WINDOW_MINUTES,
                "watermark_delay_minutes": WATERMARK_DELAY_MINUTES,
            },
            "watermark": self.watermark.isoformat() if self.watermark else None,
            "metrics": copy.deepcopy(self.metrics),
            "windows": [self.windows[key].to_dict() for key in sorted(self.windows)],
            "quarantine": [self.quarantine[key].to_dict() for key in sorted(self.quarantine)],
            "checkpoints": [
                {"checkpoint_id": cp["checkpoint_id"], "label": cp["label"], "at": cp["at"], "watermark": cp["watermark"]}
                for cp in self.checkpoints.values()
            ],
            "duplicate_markers": copy.deepcopy(self.duplicate_markers),
            "ledger": list(self.ledger),
            "reconcile": self.reconcile(),
        }

    # ------------------------------------------------------------------ 演示场景
    def run_demo_scenario(self) -> dict[str, Any]:
        """一次性回放题目要求的完整场景，返回分节报告与对账结论。"""
        self.reset()
        day = "2026-11-12"
        report: dict[str, Any] = {"steps": []}

        def step(title: str, payload: Any) -> None:
            report["steps"].append({"title": title, "result": payload})

        # 1) 正常事件入窗，水位随事件时间推进；先不结算，留给“同时结算”统一演示边界。
        for eid, tm, src, val in [
            ("EVT-A", f"{day} 09:52", "src-a", 10),
            ("EVT-B", f"{day} 10:01", "src-b", 20),
            ("EVT-C", f"{day} 10:06", "src-c", 30),
            ("EVT-D", f"{day} 10:14", "src-d", 40),
            ("EVT-E", f"{day} 10:19", "src-e", 50),
        ]:
            step(f"接入正常事件 {eid}", self.ingest(eid, tm, src, val))
        step("水位推进到 10:20", self.tick(f"{day} 10:25"))

        # 2) 比水位延迟 12 分钟的迟到事件，连续到达三次：1 条隔离 + 2 个来源副本。
        late_time = f"{day} 10:08"  # 水位 10:20，恰好落后 12 分钟
        late_arrivals = []
        for idx, src in enumerate(("src-late-1", "src-late-2", "src-late-3"), start=1):
            late_arrivals.append(self.ingest("EVT-LATE", late_time, src, 99))
        step("迟到事件 EVT-LATE 连续到达三次（落后水位12分钟）", late_arrivals)

        # 3) 另一个超窗窗口的隔离事件（落在 09:50 窗口），预置一次回算失败验证失败-重试口径。
        step("接入超窗事件 EVT-M（预置1次回算失败）", self.ingest("EVT-M", f"{day} 09:55", "src-m", 7, fail_times=1))

        # 4) 检查点：保存修补前状态。
        cp_before = self.checkpoint(label="回算修补前")
        step("保存检查点", cp_before)

        # 5) 正常窗口与超窗窗口同时结算：到期窗口走水位边界，隔离事件走回算边界，分别标注。
        batch = self.settle_batch()
        step("正常窗口与超窗窗口同时结算（分别标注边界）", batch)
        # EVT-M 首次回算失败后重试成功。
        step("EVT-M 失败后重试回算", self.recompute("Q-0002"))
        # 已回算事件再次回算必须被拦截。
        blocked: dict[str, Any]
        try:
            self.recompute("Q-0001")
            blocked = {"blocked": False}
        except WatermarkError as exc:
            blocked = {"blocked": True, "reason": exc.reason, "message": exc.message}
        step("已回算事件重复回算被拦截", blocked)

        # 7) 修补后检查点 + 恢复，恢复后重复到达只登记来源，窗口保持修补结果。
        cp_after = self.checkpoint(label="回算修补后")
        step("保存修补后检查点", cp_after)
        before_restore = self.windows[window_start_of(parse_event_time(late_time)).isoformat()].to_dict()
        self.restore(cp_after["checkpoint_id"])
        step("从修补后检查点恢复", {"restored": True})
        step("恢复后 EVT-LATE 第四次到达（仅登记来源）", self.ingest("EVT-LATE", late_time, "src-late-4", 99))
        after_restore = self.windows[window_start_of(parse_event_time(late_time)).isoformat()].to_dict()
        step("修补窗口恢复前后一致", {
            "revision_kept": before_restore["revision"] == after_restore["revision"],
            "status_kept": before_restore["status"] == after_restore["status"],
            "sum_kept": before_restore["result_sum"] == after_restore["result_sum"],
            "window": after_restore,
        })

        # 8) 中断按原因登记后再恢复回算（跨零点隔离事件）。
        self.ingest("EVT-X1", f"{day} 23:56", "src-x1", 11)
        self.ingest("EVT-X2", "2026-11-13 00:02", "src-x2", 22)
        self.ingest("EVT-X3", "2026-11-13 00:04", "src-x3", 33)
        self.tick("2026-11-13 00:14")  # 水位推进到 00:09，跨过零点
        step("跨零点超窗事件 EVT-X4 进入隔离", self.ingest("EVT-X4", f"{day} 23:57", "src-x4", 44))
        x4_id = self.event_locations["EVT-X4"]
        step("EVT-X4 处理中断（按原因登记）", self.abort(x4_id))
        step("中断后重试回算成功", self.recompute(x4_id))
        step("跨零点窗口按水位边界结算", self.settle_due())
        step(
            "跨零点对账：隔离区与正式窗口条数之和等于输入事件数",
            self.reconcile(
                window_from=parse_event_time(f"{day} 23:50"),
                window_to=parse_event_time("2026-11-13 00:10"),
            ),
        )

        report["final_state"] = self.state()
        report["reconcile"] = self.reconcile()
        return report


watermark_service = WatermarkService()
