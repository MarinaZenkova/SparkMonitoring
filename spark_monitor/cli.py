from __future__ import annotations

import argparse
from pathlib import Path

from .report import build_report, write_report
from .server import serve
from .storage import persist_report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="spark-monitor", description="Deterministic Spark Event Log analyzer")
    commands = parser.add_subparsers(dest="command", required=True)

    analyze = commands.add_parser("parse", help="Parse an Event Log and build a report")
    analyze.add_argument("event_log", help="File, compressed file, ZIP archive, or rolling-log directory")
    analyze.add_argument("--output", help="Output JSON path; defaults to data/reports/<application-id>.json")
    analyze.add_argument("--rules", help="Versioned rules JSON path")
    analyze.add_argument("--db", help="Optional SQLite database path")

    web = commands.add_parser("serve", help="Serve the local dashboard")
    web.add_argument("--reports-dir", default="data/reports")
    web.add_argument("--host", default="127.0.0.1")
    web.add_argument("--port", type=int, default=8765)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "parse":
        report = build_report(args.event_log, args.rules)
        application_id = report["application"].get("application_id") or "unknown-application"
        output = Path(args.output) if args.output else Path("data/reports") / f"{application_id}.json"
        write_report(report, output)
        if args.db:
            persist_report(report, args.db)
        print(f"Report: {output.resolve()}")
        print(
            f"Application: {report['application'].get('application_name')} | "
            f"status={report['application'].get('status')} | "
            f"problems={report['problem_count']} | primary={report['primary_evidence'] or 'none'}"
        )
        return
    serve(args.reports_dir, args.host, args.port)

