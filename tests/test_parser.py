from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from spark_monitor.report import build_report
from spark_monitor.server import AnalysisJobs
from spark_monitor.shs import SparkHistoryClient, validate_dag_name
from spark_monitor.sql_analysis import _filters, _pruning_status


def event(name: str, **values: object) -> str:
    return json.dumps({"Event": name, **values}, separators=(",", ":"))


class ParserTest(unittest.TestCase):
    def _write_log(self, lines: list[str]) -> Path:
        temp = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".inprogress", delete=False)
        with temp:
            temp.write("\n".join(lines))
        self.addCleanup(Path(temp.name).unlink, missing_ok=True)
        return Path(temp.name)

    def test_cpu_units_resources_and_partial_log(self) -> None:
        lines = [
            event("SparkListenerLogStart", **{"Spark Version": "3.5.4"}),
            event(
                "SparkListenerEnvironmentUpdate",
                **{
                    "Spark Properties": [
                        ["spark.executor.cores", "4"],
                        ["spark.dynamicAllocation.enabled", "true"],
                        ["spark.some.secret", "do-not-store"],
                    ]
                },
            ),
            event(
                "SparkListenerResourceProfileAdded",
                **{
                    "Resource Profile Id": 0,
                    "Executor Resource Requests": {"cores": {"Amount": 4}},
                    "Task Resource Requests": {"cpus": {"Amount": 1}},
                },
            ),
            event(
                "SparkListenerApplicationStart",
                **{"App ID": "app-1", "App Name": "dag__task", "Timestamp": 1000, "User": "test"},
            ),
            event(
                "SparkListenerExecutorAdded",
                **{
                    "Timestamp": 1000,
                    "Executor ID": "1",
                    "Executor Info": {"Host": "host", "Total Cores": 4, "Resource Profile Id": 0},
                },
            ),
            event(
                "SparkListenerStageSubmitted",
                **{
                    "Stage Info": {
                        "Stage ID": 2,
                        "Stage Attempt ID": 0,
                        "Stage Name": "stage",
                        "Number of Tasks": 1,
                        "Submission Time": 1000,
                    }
                },
            ),
            event(
                "SparkListenerTaskEnd",
                **{
                    "Stage ID": 2,
                    "Stage Attempt ID": 0,
                    "Task End Reason": "Success",
                    "Task Info": {
                        "Task ID": 7,
                        "Partition ID": 0,
                        "Attempt": 0,
                        "Executor ID": "1",
                        "Host": "host",
                        "Launch Time": 1000,
                        "Finish Time": 2000,
                        "Failed": False,
                        "Killed": False,
                        "Speculative": False,
                        "Accumulables": [],
                    },
                    "Task Metrics": {
                        "Executor Run Time": 1000,
                        "Executor CPU Time": 500000000,
                        "JVM GC Time": 100,
                        "Memory Bytes Spilled": 0,
                        "Disk Bytes Spilled": 0,
                        "Peak Execution Memory": 256,
                        "Input Metrics": {"Bytes Read": 100, "Records Read": 10},
                        "Output Metrics": {"Bytes Written": 20, "Records Written": 2},
                        "Shuffle Read Metrics": {
                            "Remote Bytes Read": 30,
                            "Local Bytes Read": 10,
                            "Fetch Wait Time": 20,
                            "Total Records Read": 4,
                        },
                        "Shuffle Write Metrics": {"Shuffle Bytes Written": 50, "Shuffle Records Written": 5},
                    },
                },
            ),
            event(
                "SparkListenerStageCompleted",
                **{
                    "Stage Info": {
                        "Stage ID": 2,
                        "Stage Attempt ID": 0,
                        "Stage Name": "stage",
                        "Number of Tasks": 1,
                        "Submission Time": 1000,
                        "Completion Time": 2000,
                    }
                },
            ),
            "{truncated",
        ]
        report = build_report(self._write_log(lines))
        self.assertEqual(report["application"]["analysis_status"], "PARTIAL")
        self.assertEqual(report["summary_metrics"]["executor_cpu_time_ms"], 500)
        self.assertEqual(report["summary_metrics"]["task_cpu_efficiency_pct"], 50)
        self.assertEqual(report["summary_metrics"]["task_slot_utilization_pct"], 25)
        self.assertEqual(report["summary_metrics"]["allocated_cpu_utilization_pct"], 12.5)
        self.assertTrue(report["summary_metrics"]["cpu_slot_identity_applicable"])
        self.assertEqual(report["summary_metrics"]["compute_window_duration_ms"], report["summary_metrics"]["duration_ms"])
        self.assertAlmostEqual(
            report["summary_metrics"]["allocated_cpu_utilization_pct"],
            report["summary_metrics"]["task_slot_utilization_pct"]
            * report["summary_metrics"]["task_cpu_efficiency_pct"]
            / 100,
        )
        self.assertAlmostEqual(report["summary_metrics"]["cpu_slot_identity_delta_pct_points"], 0)
        self.assertEqual(report["spark_properties"]["spark.some.secret"], "[REDACTED]")
        self.assertEqual(report["source"]["issues"][0]["line_number"], 9)

    def test_same_input_is_deterministic(self) -> None:
        path = self._write_log(
            [
                event("SparkListenerLogStart", **{"Spark Version": "3.5.4"}),
                event(
                    "SparkListenerApplicationStart",
                    **{"App ID": "app-d", "App Name": "dag__deterministic", "Timestamp": 1000, "User": "test"},
                ),
                event("SparkListenerApplicationEnd", Timestamp=2000),
            ]
        )
        self.assertEqual(build_report(path), build_report(path))

    def test_dag_validation(self) -> None:
        self.assertEqual(validate_dag_name("retail_analytics_demo"), "retail_analytics_demo")
        with self.assertRaises(ValueError):
            validate_dag_name("../../bad")

    def test_latest_run_grouping(self) -> None:
        client = SparkHistoryClient("https://history.example")
        client.applications = lambda limit=1000: [  # type: ignore[method-assign]
            {
                "id": "old-1",
                "name": "dag__a",
                "attempts": [{"startTime": "2026-01-01T10:00:00.000GMT", "endTime": "2026-01-01T10:05:00.000GMT", "duration": 300000, "completed": True}],
            },
            {
                "id": "new-1",
                "name": "dag__a",
                "attempts": [{"startTime": "2026-01-01T12:00:00.000GMT", "endTime": "2026-01-01T12:10:00.000GMT", "duration": 600000, "completed": True}],
            },
            {
                "id": "new-2",
                "name": "dag__b",
                "attempts": [{"startTime": "2026-01-01T12:04:00.000GMT", "endTime": "2026-01-01T12:12:00.000GMT", "duration": 480000, "completed": True}],
            },
            {
                "id": "other",
                "name": "other_dag__a",
                "attempts": [{"startTime": "2026-01-01T12:05:00.000GMT", "endTime": "2026-01-01T12:06:00.000GMT", "duration": 60000, "completed": True}],
            },
        ]
        self.assertEqual([item.application_id for item in client.latest_dag_run("dag")], ["new-1", "new-2"])

    def test_dataset_switch_replaces_every_previous_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            data_root = workspace / "data"
            report_root = data_root / "reports"
            raw_root = data_root / "raw"
            database_path = workspace / "spark_monitor.db"
            report_root.mkdir(parents=True)
            (report_root / "old-report.json").write_text("old report", encoding="utf-8")
            (raw_root / "old_dag").mkdir(parents=True)
            (raw_root / "old_dag" / "old.zip").write_text("old log", encoding="utf-8")
            database_path.write_text("old database", encoding="utf-8")
            (data_root / "latest_run_manifest.json").write_text("old manifest", encoding="utf-8")

            jobs = AnalysisJobs(report_root, database_path)
            staging = data_root / ".analysis-work" / "job-1"
            staged_raw = staging / "raw"
            staged_reports = staging / "reports"
            staged_raw.mkdir(parents=True)
            staged_reports.mkdir(parents=True)
            (staged_raw / "new_dag").mkdir()
            (staged_raw / "new_dag" / "new.zip").write_text("new log", encoding="utf-8")
            (staged_reports / "new-report.json").write_text("new report", encoding="utf-8")
            staged_database = staging / "spark_monitor.db"
            staged_database.write_text("new database", encoding="utf-8")
            staged_manifest = staging / "latest_run_manifest.json"
            staged_manifest.write_text("new manifest", encoding="utf-8")

            jobs._replace_current_dataset(
                "job-1",
                staged_raw,
                staged_reports,
                staged_database,
                staged_manifest,
            )

            self.assertFalse((raw_root / "old_dag").exists())
            self.assertFalse((report_root / "old-report.json").exists())
            self.assertEqual((raw_root / "new_dag" / "new.zip").read_text(encoding="utf-8"), "new log")
            self.assertEqual((report_root / "new-report.json").read_text(encoding="utf-8"), "new report")
            self.assertEqual(database_path.read_text(encoding="utf-8"), "new database")
            self.assertEqual((data_root / "latest_run_manifest.json").read_text(encoding="utf-8"), "new manifest")
            self.assertFalse((data_root / ".analysis-backup-job-1").exists())

    def test_iceberg_skipped_files_are_direct_pruning_evidence(self) -> None:
        description = (
            "BatchScan table [filters=mt_actual_flag = true, "
            "mt_load_fact_dttm >= 1789603200000000, groupedBy=]"
        )
        partition_filters, data_filters, pushed_filters, scan_filters = _filters(description)
        self.assertIsNone(partition_filters)
        self.assertIsNone(pushed_filters)
        self.assertEqual(data_filters, scan_filters)
        self.assertIn("mt_load_fact_dttm", scan_filters or "")
        status, evidence = _pruning_status(None, False, None, 176, 2)
        self.assertEqual(status, "PRUNING_DETECTED")
        self.assertIn("source_skipped_data_files", evidence)

    def test_zero_skipped_files_are_not_reported_as_successful_pruning(self) -> None:
        status, evidence = _pruning_status(None, False, None, 0, 10)
        self.assertEqual(status, "NO_PRUNING_DETECTED")
        self.assertEqual(evidence, ["source_reported_zero_skipped_data_files"])


if __name__ == "__main__":
    unittest.main()
