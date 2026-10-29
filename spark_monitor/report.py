from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from . import PARSER_VERSION, SCHEMA_VERSION
from .config import load_rules
from .metrics import calculate_metrics
from .normalizer import normalize_reader
from .reader import EventReader
from .rules import apply_rules


def build_report(log_path: str | Path, rules_path: str | Path | None = None) -> dict[str, Any]:
    rules = load_rules(rules_path)
    reader = EventReader(log_path)
    model = normalize_reader(reader)
    facts = calculate_metrics(model, rules)
    rule_result = apply_rules(facts, rules)
    return {
        "schema_version": SCHEMA_VERSION,
        "parser_version": PARSER_VERSION,
        "rules_version": str(rules["rules_version"]),
        "threshold_config": rules,
        "application": facts["application"],
        "coverage": facts["coverage"],
        "configuration": facts["configuration"],
        "spark_properties": facts["spark_properties"],
        "resources": facts["resources"],
        "summary_metrics": facts["summary_metrics"],
        "top_stages": facts["top_stages"],
        "diagnostics": rule_result["diagnostics"],
        "primary_evidence": rule_result["primary_evidence"],
        "problem_count": rule_result["problem_count"],
        "sql_executions": facts["sql_executions"],
        "sql_facts": facts["sql_facts"],
        "stages": facts["stages"],
        "source": facts["source"],
        "normalized": facts["_normalized"],
    }


def write_report(report: dict[str, Any], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n"
    path.write_text(serialized, encoding="utf-8")
    return path
