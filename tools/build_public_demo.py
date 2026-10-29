from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import re
from pathlib import Path
from typing import Any


PRIVATE_IP = re.compile(r"\b(?:10|127)\.\d{1,3}\.\d{1,3}\.\d{1,3}\b|\b192\.168\.\d{1,3}\.\d{1,3}\b|\b172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}\b")
EMAIL = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
S3_URI = re.compile(r"s3a?://[^/\s,\"]+", re.IGNORECASE)
INTERNAL_URL = re.compile(r"https?://[A-Z0-9.-]+\.internal", re.IGNORECASE)
SERVICE_HOST = re.compile(r"[A-Z0-9-]+(?:\.[A-Z0-9-]+)*\.svc(?:\.cluster\.local)?", re.IGNORECASE)
CATALOG_TABLE = re.compile(r"nessie\.[A-Z0-9_]+(?:\.[A-Z0-9_]+)+", re.IGNORECASE)
REGISTRY_IMAGE = re.compile(r"registry\.[A-Z0-9.-]+/[A-Z0-9_./-]+:[A-Z0-9_.-]+", re.IGNORECASE)
SPARK_APP_ID = re.compile(r"spark-[0-9a-f]{32}\b", re.IGNORECASE)
UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b", re.IGNORECASE)
ETL_JAR = re.compile(r"[A-Z0-9_-]+-etl_2[.]12-[0-9.]+[.]jar", re.IGNORECASE)
ETL_PATH = re.compile(r"[A-Z0-9_-]+-etl/[0-9.]+", re.IGNORECASE)
SENSITIVE_KEY = re.compile(
    r"password|passwd|secret|token|credential|access[._-]?key|private[._-]?key|client[._-]?secret",
    re.IGNORECASE,
)


def stable_number(value: str, upper: int) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % upper + 1


def demo_ip(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return value
    if not (address.is_private or address.is_loopback):
        return value
    return f"192.0.2.{stable_number(value, 254)}"


def replace_match(pattern: re.Pattern[str], text: str, prefix: str) -> str:
    return pattern.sub(lambda match: f"{prefix}{stable_number(match.group(0), 999):03d}", text)


def sanitize_string(value: str) -> str:
    value = PRIVATE_IP.sub(lambda match: demo_ip(match.group(0)), value)
    value = EMAIL.sub("demo-user@example.invalid", value)
    value = S3_URI.sub("s3a://demo-lakehouse", value)
    value = INTERNAL_URL.sub("https://services.demo.invalid", value)
    value = REGISTRY_IMAGE.sub("registry.demo.invalid/platform/spark:3.5.4", value)
    value = SERVICE_HOST.sub(
        lambda match: f"spark-driver-{stable_number(match.group(0), 999):03d}.demo.svc.cluster.local",
        value,
    )
    value = replace_match(CATALOG_TABLE, value, "demo_catalog.retail.table_")
    value = replace_match(SPARK_APP_ID, value, "spark-demo-")
    value = ETL_JAR.sub("retail-demo-etl_2.12-0.1.0.jar", value)
    value = ETL_PATH.sub("retail-demo-etl/0.1.0", value)
    value = UUID.sub(
        lambda match: f"00000000-0000-4000-8000-{stable_number(match.group(0), 10**12 - 1):012d}",
        value,
    )
    return value


def sanitize(value: Any, key: str = "") -> Any:
    if SENSITIVE_KEY.search(key):
        return "[REDACTED]" if value is not None else None
    if isinstance(value, dict):
        return {item_key: sanitize(item_value, item_key) for item_key, item_value in value.items()}
    if isinstance(value, list):
        return [sanitize(item, key) for item in value]
    if isinstance(value, str):
        if key == "spark.master":
            return "k8s://https://kubernetes.demo.svc.cluster.local"
        if key == "spark.hadoop.fs.s3a.endpoint":
            return "https://object-storage.demo.invalid"
        if "namespace" in key.lower():
            return "demo-workloads"
        if "container.image" in key.lower():
            return "registry.demo.invalid/platform/spark:3.5.4"
        return sanitize_string(value)
    return value


def validate_public_data(value: Any, key: str = "root") -> None:
    if isinstance(value, dict):
        for item_key, item_value in value.items():
            if SENSITIVE_KEY.search(item_key) and item_value not in (None, "[REDACTED]"):
                raise ValueError(f"Sensitive value was not redacted: {item_key}")
            validate_public_data(item_value, item_key)
        return
    if isinstance(value, list):
        for item in value:
            validate_public_data(item, key)
        return
    if not isinstance(value, str):
        return
    if PRIVATE_IP.search(value):
        raise ValueError(f"Private IP remains in {key}")
    if re.search(r"https?://[A-Z0-9.-]+[.]internal\b", value, re.IGNORECASE):
        raise ValueError(f"Internal URL remains in {key}")
    if S3_URI.search(value) and "s3a://demo-lakehouse" not in value:
        raise ValueError(f"Non-demo S3 URI remains in {key}")


def choose_reports(reports: list[tuple[Path, dict[str, Any]]], limit: int) -> list[tuple[Path, dict[str, Any]]]:
    markers = ("00_", "01_", "02_", "publish")
    selected: list[tuple[Path, dict[str, Any]]] = []
    for marker in markers:
        matches = [
            item
            for item in reports
            if marker in str(item[1].get("application", {}).get("application_name", ""))
        ]
        if matches:
            selected.append(min(matches, key=lambda item: item[0].stat().st_size))
    for item in sorted(reports, key=lambda candidate: candidate[0].stat().st_size):
        if item not in selected:
            selected.append(item)
        if len(selected) >= limit:
            break
    return selected[:limit]


def build_demo(source: Path, target: Path, limit: int) -> None:
    reports = []
    for path in source.glob("spark-*.json"):
        reports.append((path, json.loads(path.read_text(encoding="utf-8"))))
    if not reports:
        raise SystemExit(f"No Spark reports found in {source}")

    target.mkdir(parents=True, exist_ok=True)
    for old_report in target.glob("*.json"):
        old_report.unlink()
    selected = choose_reports(reports, limit)
    public_reports = []
    for path, report in selected:
        public_report = sanitize(report)
        validate_public_data(public_report)
        public_reports.append(public_report)
        output = target / f"{public_report['application']['application_id']}.json"
        output.write_text(
            json.dumps(public_report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    applications = [report["application"] for report in public_reports]
    dashboard = {
        "application_count": len(applications),
        "dag_name": "retail_analytics_demo",
        "report_ids": [application["application_id"] for application in applications],
        "run_end": max(application.get("end_time") or 0 for application in applications),
        "run_start": min(application.get("start_time") or 0 for application in applications),
        "shs_url": "https://spark-history.demo.invalid",
        "successful_report_count": sum(application.get("status") == "SUCCEEDED" for application in applications),
    }
    (target / "latest_dashboard.json").write_text(
        json.dumps(dashboard, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Public demo: {len(public_reports)} reports -> {target.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a sanitized report set for the public demo")
    parser.add_argument("--source", type=Path, default=Path("data/reports"))
    parser.add_argument("--target", type=Path, default=Path("demo/reports"))
    parser.add_argument("--limit", type=int, default=4)
    args = parser.parse_args()
    build_demo(args.source, args.target, max(1, args.limit))


if __name__ == "__main__":
    main()
