"""水位边界与迟到治理引擎的确定性测试。

直接对 WatermarkService 做断言，覆盖：
超窗隔离不丢弃、重复事件只算一次、回算仅一次、失败/重试/中断按原因处理、
检查点恢复不回退修补结果、跨零点归属稳定、质量台账与流处理作业跨模块同步、对账恒等式。
"""
from __future__ import annotations

import unittest
from datetime import datetime

from app.services.watermark import (
    BOUNDARY_RECOMPUTE,
    BOUNDARY_WATERMARK,
    Q_ABORTED,
    Q_FAILED,
    Q_RESOLVED,
    REASON_MANUAL_ABORT,
    REASON_RECOMPUTE_FAILED,
    REASON_REPEAT_RECOMPUTE,
    WINDOW_PATCHED,
    WINDOW_SETTLED,
    WatermarkError,
    WatermarkService,
    parse_event_time,
    window_start_of,
)
from app.store import store

DAY = "2026-11-12"


class WatermarkEngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = WatermarkService()

    # 1) 比水位延迟 12 分钟的迟到事件进入隔离清单，而不是静默丢弃。
    def test_late_event_quarantined_not_dropped(self) -> None:
        self.svc.ingest("E1", f"{DAY} 10:01", "s1", 10)
        self.svc.ingest("E2", f"{DAY} 10:06", "s2", 20)
        self.svc.tick(f"{DAY} 10:20")  # 水位 10:15
        self.svc.settle_due()

        result = self.svc.ingest("LATE", f"{DAY} 10:03", "s-late", 99)
        self.assertEqual(result["disposition"], "quarantined")
        self.assertEqual(result["lag_minutes"], 12)

        records = self.svc.state()["quarantine"]
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["event_id"], "LATE")
        self.assertEqual(record["lag_minutes"], 12)
        self.assertEqual(record["original_event"]["value"], 99)  # 原始事件可追溯
        # 输入事件没有静默消失：对账恒等式成立。
        self.assertTrue(self.svc.reconcile()["all_passed"])

    # 2) 同一事件连续到达三次：只计算一次，后续副本仅登记来源标识。
    def test_duplicate_arrivals_first_only(self) -> None:
        self.svc.ingest("D1", f"{DAY} 09:52", "a", 5)
        self.svc.tick(f"{DAY} 10:20")
        self.svc.settle_due()

        arrivals = [
            self.svc.ingest("DUP", f"{DAY} 10:08", "src-1", 7),
            self.svc.ingest("DUP", f"{DAY} 10:08", "src-2", 7),
            self.svc.ingest("DUP", f"{DAY} 10:08", "src-3", 7),
        ]
        self.assertEqual(arrivals[0]["disposition"], "quarantined")
        self.assertEqual(arrivals[1]["disposition"], "duplicate_marker_only")
        self.assertEqual(arrivals[2]["disposition"], "duplicate_marker_only")

        state = self.svc.state()
        self.assertEqual(state["metrics"]["quarantine_total"], 1)
        self.assertEqual(state["metrics"]["duplicate_arrivals"], 2)
        self.assertEqual(state["metrics"]["distinct_events"], 2)
        markers = state["duplicate_markers"]["DUP"]
        self.assertEqual([m["source_id"] for m in markers], ["src-2", "src-3"])

    # 3) 首次回算可执行一次，重复回算被拦截；已修补窗口修订版本只增不退。
    def test_recompute_only_once_and_revision_advances(self) -> None:
        self.svc.ingest("W1", f"{DAY} 10:01", "a", 20)
        self.svc.ingest("W2", f"{DAY} 10:06", "b", 30)
        self.svc.tick(f"{DAY} 10:20")
        self.svc.settle_due()
        self.svc.ingest("L", f"{DAY} 10:08", "late", 99)
        qid = self.svc.state()["quarantine"][0]["quarantine_id"]

        window = self.svc.recompute(qid)
        self.assertEqual(window["status"], WINDOW_PATCHED)
        self.assertEqual(window["boundary"], BOUNDARY_RECOMPUTE)
        self.assertEqual(window["revision"], 2)
        self.assertEqual(window["result_sum"], 149)  # 20 + 30 + 99
        self.assertEqual(window["result_count"], 3)

        with self.assertRaises(WatermarkError) as ctx:
            self.svc.recompute(qid)
        self.assertEqual(ctx.exception.reason, REASON_REPEAT_RECOMPUTE)
        # 拦截后窗口仍是修补后的结果，没有被旧值覆盖。
        again = self.svc.state()["windows"]
        target = next(w for w in again if w["window_start"] == window["window_start"])
        self.assertEqual(target["revision"], 2)
        self.assertEqual(target["result_sum"], 149)

    # 4) 检查点恢复后，已修补窗口仍是修补后的结果，不退回旧值；副本仍只登记。
    def test_checkpoint_restore_keeps_patched_window(self) -> None:
        self.svc.ingest("W1", f"{DAY} 10:01", "a", 20)
        self.svc.tick(f"{DAY} 10:20")
        self.svc.settle_due()
        self.svc.ingest("L", f"{DAY} 10:05", "late", 40)
        qid = self.svc.state()["quarantine"][0]["quarantine_id"]
        patched = self.svc.recompute(qid)
        cp = self.svc.checkpoint(label="修补后")

        self.svc.reset()
        self.assertEqual(self.svc.state()["windows"], [])  # 已清空

        self.svc.restore(cp["checkpoint_id"])
        restored = next(w for w in self.svc.state()["windows"] if w["window_start"] == patched["window_start"])
        self.assertEqual(restored["status"], WINDOW_PATCHED)
        self.assertEqual(restored["revision"], 2)
        self.assertEqual(restored["result_sum"], 60)  # 20 + 40，不是修补前的 20

        # 恢复后重复到达只登记来源，窗口结果不变。
        dup = self.svc.ingest("L", f"{DAY} 10:05", "late-copy", 40)
        self.assertEqual(dup["disposition"], "duplicate_marker_only")
        final = next(w for w in self.svc.state()["windows"] if w["window_start"] == patched["window_start"])
        self.assertEqual(final["result_sum"], 60)
        self.assertEqual(final["revision"], 2)

    # 5) 回算失败按原因登记可重试，重试成功。
    def test_failure_then_retry(self) -> None:
        self.svc.ingest("W1", f"{DAY} 10:01", "a", 1)
        self.svc.tick(f"{DAY} 10:20")
        self.svc.settle_due()
        self.svc.ingest("F", f"{DAY} 10:02", "f", 9, fail_times=1)
        qid = self.svc.state()["quarantine"][0]["quarantine_id"]

        with self.assertRaises(WatermarkError) as fail_ctx:
            self.svc.recompute(qid)
        self.assertEqual(fail_ctx.exception.reason, REASON_RECOMPUTE_FAILED)
        self.assertEqual(self.svc.state()["quarantine"][0]["status"], Q_FAILED)
        self.assertEqual(self.svc.state()["metrics"]["failure_by_reason"][REASON_RECOMPUTE_FAILED], 1)

        window = self.svc.recompute(qid)  # 重试成功
        self.assertEqual(window["status"], WINDOW_PATCHED)
        self.assertEqual(self.svc.state()["quarantine"][0]["status"], Q_RESOLVED)

    # 6) 中断按原因落账，之后仍可恢复回算。
    def test_abort_then_retry(self) -> None:
        self.svc.ingest("W1", f"{DAY} 10:01", "a", 1)
        self.svc.tick(f"{DAY} 10:20")
        self.svc.settle_due()
        self.svc.ingest("X", f"{DAY} 10:03", "x", 8)
        qid = self.svc.state()["quarantine"][0]["quarantine_id"]

        record = self.svc.abort(qid)
        self.assertEqual(record["status"], Q_ABORTED)
        self.assertEqual(record["attempts"][-1]["reason"], REASON_MANUAL_ABORT)
        self.assertEqual(self.svc.state()["metrics"]["failure_by_reason"][REASON_MANUAL_ABORT], 1)

        window = self.svc.recompute(qid)
        self.assertEqual(window["status"], WINDOW_PATCHED)
        self.assertEqual(self.svc.state()["quarantine"][0]["status"], Q_RESOLVED)

    # 7) 正常窗口与超窗窗口同时结算，分别标注各自采用的边界。
    def test_batch_settlement_boundaries(self) -> None:
        self.svc.ingest("N1", f"{DAY} 09:52", "n", 10)
        self.svc.ingest("N2", f"{DAY} 10:04", "n", 20)
        self.svc.ingest("N3", f"{DAY} 10:14", "n", 30)
        self.svc.tick(f"{DAY} 10:25")  # 水位 10:20，W09、W10 到期
        self.svc.ingest("L1", f"{DAY} 10:08", "l", 99)  # 超窗进隔离
        batch = self.svc.settle_batch()

        normal_keys = {w["window_start"] for w in batch["watermark_boundary_windows"]}
        self.assertIn(parse_event_time(f"{DAY} 09:50").isoformat(), normal_keys)
        self.assertIn(parse_event_time(f"{DAY} 10:00").isoformat(), normal_keys)
        for win in batch["watermark_boundary_windows"]:
            self.assertEqual(win["boundary"], BOUNDARY_WATERMARK)
            self.assertEqual(win["status"], WINDOW_SETTLED)

        patched = batch["recompute_boundary_windows"]
        self.assertEqual(len(patched), 1)
        self.assertEqual(patched[0]["boundary"], BOUNDARY_RECOMPUTE)
        self.assertEqual(patched[0]["status"], WINDOW_PATCHED)
        self.assertEqual(batch["failures"], [])

    # 8) 跨零点窗口：水位推进前后事件归属稳定，隔离+正式条数之和等于输入事件数。
    def test_cross_midnight_attribution_and_reconcile(self) -> None:
        self.svc.ingest("X1", f"{DAY} 23:56", "x", 11)
        before_window = window_start_of(parse_event_time(f"{DAY} 23:56")).isoformat()
        self.svc.ingest("X2", "2026-11-13 00:02", "x", 22)
        self.svc.ingest("X3", "2026-11-13 00:04", "x", 33)
        self.svc.tick("2026-11-13 00:14")  # 水位推进到 00:09，跨过零点

        # 水位推进前入窗的事件归属稳定：X1 仍在 23:50 窗口，X2/X3 在 00:00 窗口。
        state = self.svc.state()
        before_events = {
            e["event_id"]
            for w in state["windows"] if w["window_start"] == before_window for e in w["events"]
        }
        self.assertEqual(before_events, {"X1"})
        after_window = next(w for w in state["windows"] if w["window_start"] == "2026-11-13T00:00:00")
        self.assertEqual({e["event_id"] for e in after_window["events"]}, {"X2", "X3"})

        late = self.svc.ingest("X4", f"{DAY} 23:57", "x", 44)
        self.assertEqual(late["disposition"], "quarantined")
        qid = late["quarantine_id"]
        self.svc.recompute(qid)
        self.svc.settle_due()

        scope = self.svc.reconcile(
            window_from=parse_event_time(f"{DAY} 23:50"),
            window_to=parse_event_time("2026-11-13 00:10"),
        )
        self.assertTrue(scope["all_passed"])
        # 4 个跨零点输入事件全部在正式窗口，无丢失、无重叠。
        self.assertEqual(scope["distinct_input_events"], 4)
        self.assertEqual(scope["formal_window_events"] + scope["quarantine_primary_records"], 4)

    # 9) 隔离数量、失败原因与质量指标同步到质量台账和流处理作业状态。
    def test_quality_ledger_and_stream_job_sync(self) -> None:
        self.svc.ingest("E", f"{DAY} 10:01", "a", 1)
        self.svc.tick(f"{DAY} 10:20")
        self.svc.settle_due()
        self.svc.ingest("Q1", f"{DAY} 10:02", "q", 1, fail_times=1)
        with self.assertRaises(WatermarkError):
            self.svc.recompute("Q-0001")

        quality = next(row for row in store.rows("quality") if row.get("指标编号") == "QUAL-WM-0001")
        job = next(row for row in store.rows("stream") if row.get("作业编号") == "STRE-0003")
        self.assertEqual(quality["隔离事件数"], self.svc.metrics["quarantine_total"])
        self.assertEqual(quality["待处理隔离"], 1)
        self.assertEqual(quality["回算失败"], 1)
        self.assertEqual(quality["失败原因分布"][REASON_RECOMPUTE_FAILED], 1)
        self.assertEqual(job["隔离事件数"], quality["隔离事件数"])  # 多处隔离结果对得上
        self.assertTrue(job["abnormal"])

    # 10) 完整演示场景对账全通过：超窗不静默消失，已结算窗口不退回旧值。
    def test_full_demo_scenario_reconcile(self) -> None:
        report = self.svc.run_demo_scenario()
        self.assertTrue(report["reconcile"]["all_passed"])
        reconcile = report["reconcile"]
        self.assertEqual(reconcile["missing_events"], [])
        self.assertEqual(reconcile["overlap_events"], [])
        # 12 分钟迟到事件：1 条隔离主记录 + 3 个重复来源副本（含恢复后那次）。
        metrics = report["final_state"]["metrics"]
        self.assertEqual(metrics["duplicate_arrivals"], 3)
        self.assertEqual(metrics["quarantine_total"], 3)
        self.assertEqual(metrics["quarantine_resolved"], 3)
        # 到达口径：总到达次数 = 去重事件数 + 重复副本数，水位推进（tick）不计入到达。
        self.assertEqual(
            metrics["input_arrivals"],
            metrics["distinct_events"] + metrics["duplicate_arrivals"],
        )
        # 所有已修补窗口修订版本都未回退。
        for win in report["final_state"]["windows"]:
            if win["status"] == WINDOW_PATCHED:
                self.assertGreaterEqual(win["revision"], 2)
        # 跨零点分节对账同样通过。
        scoped = next(
            step["result"] for step in report["steps"] if step["title"].startswith("跨零点对账")
        )
        self.assertTrue(scoped["all_passed"])

    # 11) 已结算窗口的结果不会被后续动作覆盖回旧值。
    def test_settled_window_never_overwritten(self) -> None:
        self.svc.ingest("A", f"{DAY} 10:02", "a", 15)
        self.svc.tick(f"{DAY} 10:20")
        settled_list = self.svc.settle_due()
        settled = settled_list[0]
        self.assertEqual(settled["result_sum"], 15)

        # 迟到事件只能进隔离，不能改写已结算窗口。
        late = self.svc.ingest("B", f"{DAY} 10:03", "b", 25)
        self.assertEqual(late["disposition"], "quarantined")
        win = next(w for w in self.svc.state()["windows"] if w["window_start"] == settled["window_start"])
        self.assertEqual(win["result_sum"], 15)
        self.assertEqual(win["status"], WINDOW_SETTLED)


if __name__ == "__main__":
    unittest.main()
