const $ = selector => document.querySelector(selector);
const number = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 1 });
const diagnosticOrder = ["compute", "memory", "skew", "shuffle", "scan", "joins", "sql_operators"];
const diagnosticNames = {
  compute: "Compute / resources", memory: "Memory / GC / spill", skew: "Data skew / stragglers",
  shuffle: "Shuffle pressure", scan: "Scan / partition pruning", joins: "Join pathologies",
  sql_operators: "Expensive SQL operators"
};

const escapeHtml = value => String(value ?? "—").replace(/[&<>'"]/g, char => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;"
})[char]);
const available = value => value !== null && value !== undefined && Number.isFinite(Number(value));
const fmtNumber = value => available(value) ? number.format(Number(value)) : "—";
const fmtPct = value => available(value) ? `${number.format(Number(value))}%` : "—";
const fmtRatio = value => available(value) ? `${number.format(Number(value))}×` : "—";
const fmtDuration = value => {
  if (!available(value)) return "—";
  const seconds = Math.round(Number(value) / 1000), hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60), remainder = seconds % 60;
  return hours ? `${hours}ч ${minutes}м` : minutes ? `${minutes}м ${remainder}с` : `${remainder}с`;
};
const fmtSeconds = value => available(value) ? `${number.format(Number(value))} core-s` : "—";
const fmtBytes = value => {
  if (!available(value)) return "—";
  let amount = Number(value), index = 0;
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  while (Math.abs(amount) >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; }
  return `${number.format(amount)} ${units[index]}`;
};
const fmtTime = value => available(value)
  ? new Date(Number(value)).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" })
  : "—";
const yesNo = value => value === true ? "YES" : value === false ? "NO" : "—";
const statusClass = value => String(value || "NOT_AVAILABLE").toLowerCase();
const shortStatus = value => ({
  CRITICAL: "CRIT", WARNING: "WARN", OK: "OK", NOT_AVAILABLE: "UNKNOWN", NOT_APPLICABLE: "N/A",
  SUCCEEDED: "DONE", FAILED: "FAIL", INCOMPLETE: "PART"
})[String(value)] || String(value || "N/D");
const statusMark = value => `<span class="status-mark ${statusClass(value)}" title="${escapeHtml(value || "NOT AVAILABLE")}"><i></i>${escapeHtml(shortStatus(value))}</span>`;
const plainMetric = (main, sub = "") => `<div class="plain-metric"><strong>${escapeHtml(main)}</strong>${sub ? `<small>${escapeHtml(sub)}</small>` : ""}</div>`;

function diagnosticCell(block, main, sub) {
  return `<div class="diagnostic-cell">${statusMark(block?.status)}<div class="diag-values"><strong title="${escapeHtml(main)}">${escapeHtml(main)}</strong><small title="${escapeHtml(sub)}">${escapeHtml(sub)}</small></div></div>`;
}

const sumValues = (...values) => values.reduce((total, value) => total + (Number(value) || 0), 0);
const plural = (value, one, many) => `${fmtNumber(value)} ${Number(value) === 1 ? one : many}`;

function scanHeadline(block, summary) {
  const metrics = block?.metrics || {};
  if (block?.status === "NOT_APPLICABLE") return metrics.scan_count
    ? ["Source not partitioned", "Pruning not applicable"]
    : ["No scans", "Not present in physical plan"];
  if (metrics.pruning_detected_scans && !metrics.confirmed_unpruned_scans && !metrics.unknown_pruning_scans) {
    return [`${fmtPct(metrics.file_pruning_pct)} files pruned`, `${fmtBytes(summary.scan_bytes)} · ${fmtNumber(metrics.result_data_files)} selected`];
  }
  if (metrics.confirmed_unpruned_scans) {
    return [`${fmtNumber(metrics.confirmed_unpruned_scans)} scans unpruned`, `${fmtBytes(summary.scan_bytes)} · ${fmtNumber(metrics.pruning_detected_scans)} pruned`];
  }
  return ["Pruning not verified", `${fmtBytes(summary.scan_bytes)} · ${fmtNumber(metrics.unknown_pruning_scans)} unknown`];
}

function sqlHeadline(block) {
  const metrics = block?.metrics || {};
  const parts = [];
  if (metrics.window_count) parts.push(plural(metrics.window_count, "Window", "Windows"));
  if (metrics.sort_count) parts.push(plural(metrics.sort_count, "Sort", "Sorts"));
  if (metrics.exchange_count) parts.push(plural(metrics.exchange_count, "Exchange", "Exchanges"));
  if (!parts.length) return ["No expensive operators", "Physical plan checked"];
  const size = metrics.largest_exchange_bytes ? `${fmtBytes(metrics.largest_exchange_bytes)} max output` : "Sizes in details";
  return [parts.slice(0, 2).join(" · "), size];
}

function applicationRow(report) {
  const app = report.application || {}, summary = report.summary_metrics || {}, diagnostics = report.diagnostics || {};
  const joinMetrics = diagnostics.joins?.metrics || {};
  const scanText = scanHeadline(diagnostics.scan, summary);
  const sqlText = sqlHeadline(diagnostics.sql_operators);
  const spillBytes = sumValues(summary.memory_spill_bytes, summary.disk_spill_bytes);
  const reportId = escapeHtml(report.report_id);
  const taskName = String(app.application_name || "").includes("__")
    ? String(app.application_name).split("__").slice(1).join("__") : app.application_name;
  const applicationStatus = app.status || "UNKNOWN";
  const partialLog = report.coverage?.log_complete === false || app.analysis_status === "PARTIAL";
  return `<tr class="app-row" data-report="${reportId}" tabindex="0" aria-expanded="false">
    <td class="sticky"><div class="app-name"><span class="expand-icon">›</span><span class="app-state ${statusClass(applicationStatus)}" title="Spark application: ${escapeHtml(applicationStatus)}"></span><div class="app-name-text"><strong>${escapeHtml(taskName)}</strong><small>${fmtTime(app.start_time)} · ${fmtDuration(summary.duration_ms)} · <span class="app-result ${statusClass(applicationStatus)}">${escapeHtml(applicationStatus)}</span>${partialLog ? ` · <span class="log-partial" title="Event Log неполный: часть метрик может быть недоступна">PARTIAL LOG</span>` : ""}</small></div></div></td>
    <td>${diagnosticCell(diagnostics.compute, `${fmtPct(summary.allocated_cpu_utilization_pct)} CPU used`, `${fmtPct(summary.task_slot_utilization_pct)} slots`)}</td>
    <td>${diagnosticCell(diagnostics.memory, `${fmtPct(summary.gc_ratio_pct)} GC`, `${fmtBytes(spillBytes)} spill`)}</td>
    <td>${diagnosticCell(diagnostics.skew, `${fmtRatio(summary.worst_duration_skew_factor)} duration`, `${fmtRatio(summary.worst_input_skew_factor)} input`)}</td>
    <td>${diagnosticCell(diagnostics.shuffle, `${fmtBytes(summary.total_shuffle_io_bytes)} total I/O`, `${fmtPct(summary.shuffle_fetch_wait_pct)} wait`)}</td>
    <td>${diagnosticCell(diagnostics.scan, scanText[0], scanText[1])}</td>
    <td>${diagnosticCell(diagnostics.joins, joinMetrics.join_count ? plural(joinMetrics.join_count, "join", "joins") : "No joins", joinMetrics.join_count ? `${fmtRatio(joinMetrics.max_join_amplification)} max amp` : "Plan checked")}</td>
    <td>${diagnosticCell(diagnostics.sql_operators, sqlText[0], sqlText[1])}</td>
  </tr><tr class="details-row" data-details="${reportId}" hidden><td colspan="8"><div class="details"><div class="empty">Загружаю детали…</div></div></td></tr>`;
}

function configValue(report, key) {
  const item = report.configuration?.[key];
  return item?.effective_value ?? "—";
}

function configFlag(report, key) {
  const value = String(configValue(report, key)).toLowerCase();
  if (["true", "1", "yes"].includes(value)) return "ON";
  if (["false", "0", "no"].includes(value)) return "OFF";
  return value === "—" ? "—" : value.toUpperCase();
}

function metricRows(items) {
  return `<div class="metric-list">${items.map(([label, value]) => `<div><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`).join("")}</div>`;
}

function problemList(block) {
  const problems = block?.problems || [];
  if (!problems.length) return "";
  return `<div class="problem-list">${problems.map(problem => `<div class="problem ${statusClass(problem.severity)}"><strong>${escapeHtml(problem.problem_code)}</strong><span>${escapeHtml((problem.evidence || []).join(" · "))}</span></div>`).join("")}</div>`;
}

function blockMetrics(name, report) {
  const summary = report.summary_metrics || {}, block = report.diagnostics?.[name] || {}, metrics = block.metrics || {};
  if (name === "compute") return [
    ["Allocated CPU", fmtPct(summary.allocated_cpu_utilization_pct)], ["Task-slot utilization", fmtPct(summary.task_slot_utilization_pct)],
    ["Task CPU efficiency", fmtPct(summary.task_cpu_efficiency_pct)],
    ["Running / available, avg", `${fmtNumber(summary.average_running_tasks)} / ${fmtNumber(summary.average_available_slots)}`],
    ["Running / available, peak", `${fmtNumber(summary.peak_running_tasks)} / ${fmtNumber(summary.peak_available_slots)}`]
  ];
  if (name === "memory") return [
    ["GC time / ratio", `${fmtDuration(summary.gc_time_ms)} / ${fmtPct(summary.gc_ratio_pct)}`],
    ["Memory spill", fmtBytes(summary.memory_spill_bytes)], ["Disk spill", fmtBytes(summary.disk_spill_bytes)],
    ["Tasks with spill", fmtPct(summary.tasks_with_spill_pct)], ["Peak execution memory P95", fmtBytes(summary.peak_execution_memory_p95)],
    ["Peak execution memory MAX", fmtBytes(summary.peak_execution_memory_max)]
  ];
  if (name === "skew") return [
    ["Duration P95 / median", fmtRatio(summary.worst_duration_skew_factor)], ["Input P95 / median", fmtRatio(summary.worst_input_skew_factor)],
    ["Shuffle P95 / median", fmtRatio(summary.worst_shuffle_skew_factor)], ["Duration MAX / median", fmtRatio(summary.worst_duration_max_factor)],
    ["Skewed tasks", fmtPct(summary.worst_skewed_tasks_pct)], ["Worst stage", fmtNumber(summary.worst_stage_id)]
  ];
  if (name === "shuffle") return [
    ["Shuffle read", fmtBytes(summary.shuffle_read_bytes)], ["Shuffle write", fmtBytes(summary.shuffle_write_bytes)],
    ["Total shuffle", fmtBytes(summary.total_shuffle_io_bytes)], ["Fetch wait", `${fmtDuration(summary.shuffle_fetch_wait_ms)} / ${fmtPct(summary.shuffle_fetch_wait_pct)}`],
    ["Shuffle / source input", fmtRatio(summary.shuffle_to_source_input_ratio)], ["Remote read", fmtPct(summary.remote_shuffle_share_pct)]
  ];
  return [];
}

function relationLabel(value) {
  const parts = String(value || "Unknown source").split(".");
  return parts[parts.length - 1];
}

function scanEvidence(scan) {
  if (scan.partition_pruning_status === "PRUNING_DETECTED") {
    return { status: "OK", label: `${fmtPct(scan.file_pruning_pct)} pruned`, note: `${fmtNumber(scan.skipped_data_files)} skipped` };
  }
  if (["NO_PRUNING_DETECTED", "NO_PARTITION_FILTER"].includes(scan.partition_pruning_status)) {
    return { status: "WARNING", label: "No files pruned", note: `${fmtNumber(scan.result_data_files)} selected` };
  }
  if (scan.partition_pruning_status === "SOURCE_NOT_PARTITIONED") {
    return { status: "NOT_APPLICABLE", label: "Not partitioned", note: "Source metadata" };
  }
  return { status: "NOT_AVAILABLE", label: "Cannot verify", note: "No direct evidence" };
}

function scanList(scans) {
  return `<div class="scan-list">${scans.map(scan => {
    const evidence = scanEvidence(scan);
    return `<article><header><strong title="${escapeHtml(scan.table_or_relation)}">${escapeHtml(relationLabel(scan.table_or_relation))}</strong>${statusMark(evidence.status)}</header><div class="scan-facts">
      <div><span>Read</span><b>${fmtBytes(scan.scan_bytes)}</b></div><div><span>Rows</span><b>${fmtNumber(scan.scan_output_rows)}</b></div>
      <div><span>Selected</span><b>${fmtNumber(scan.result_data_files)} files</b></div><div><span>Skipped</span><b>${fmtNumber(scan.skipped_data_files)} files</b></div>
    </div><small title="${escapeHtml(scan.scan_filters || "")}">${escapeHtml(evidence.label)} · ${scan.scan_filters_present ? "filters captured" : "filter unavailable"}</small></article>`;
  }).join("")}</div>`;
}

function scanDrilldown(block) {
  const scans = block?.metrics?.scans || [];
  const metrics = block?.metrics || {};
  if (!scans.length) return `<div class="empty-state">No scans in the physical plan.</div>`;
  const sorted = scans.slice().sort((left, right) => (right.scan_bytes || 0) - (left.scan_bytes || 0));
  const confirmed = metrics.pruning_detected_scans || 0;
  const unpruned = metrics.confirmed_unpruned_scans || 0;
  const unknown = metrics.unknown_pruning_scans || 0;
  const outcome = confirmed && !unpruned && !unknown
    ? ["PRUNING CONFIRMED", `${fmtPct(metrics.file_pruning_pct)} of candidate files excluded`, "ok"]
    : unpruned
      ? ["UNPRUNED SCANS FOUND", `${unpruned} of ${scans.length} scans selected every candidate file`, "warning"]
      : ["PRUNING CANNOT BE VERIFIED", `${unknown} of ${scans.length} scans lack direct evidence`, "not_available"];
  return `<div class="pruning-outcome ${outcome[2]}"><span>${outcome[0]}</span><strong>${outcome[1]}</strong><div class="pruning-stats">
      <div><small>Read</small><b>${fmtBytes(metrics.scan_bytes)}</b></div><div><small>Rows</small><b>${fmtNumber(metrics.scan_rows)}</b></div>
      <div><small>Selected</small><b>${fmtNumber(metrics.result_data_files)}</b></div><div><small>Skipped</small><b>${fmtNumber(metrics.skipped_data_files)}</b></div>
    </div></div><p class="evidence-note">Evidence: physical-plan filters and source file counters.</p>
    <h5 class="detail-heading">Largest scans</h5>${scanList(sorted.slice(0, 2))}
    ${sorted.length > 2 ? `<details class="more-evidence"><summary>Show all ${sorted.length} scans</summary>${scanList(sorted)}</details>` : ""}`;
}

function joinDrilldown(block) {
  const joins = block?.metrics?.joins || [];
  const metrics = block?.metrics || {};
  if (!joins.length) return `<div class="empty-state"><strong>No joins in physical plan</strong><span>Join diagnostics are not applicable to this application.</span></div>`;
  const summaryRows = [["Joins", fmtNumber(metrics.join_count)]];
  if (available(metrics.max_join_amplification)) summaryRows.push(["Max row amplification", fmtRatio(metrics.max_join_amplification)]);
  const joinArticle = (join, index) => {
    const rows = [];
    if (available(join.left_rows) || available(join.right_rows)) rows.push(["Left / right rows", `${fmtNumber(join.left_rows)} / ${fmtNumber(join.right_rows)}`]);
    if (available(join.join_output_rows)) rows.push(["Output rows", fmtNumber(join.join_output_rows)]);
    if (available(join.join_row_amplification)) rows.push(["Row amplification", fmtRatio(join.join_row_amplification)]);
    if ((join.stage_ids || []).length) rows.push(["Stages", join.stage_ids.join(", ")]);
    if (typeof join.potential_broadcast_candidate === "boolean") rows.push(["Broadcast candidate", yesNo(join.potential_broadcast_candidate)]);
    return `<article><header><strong>JOIN #${index + 1} · ${escapeHtml(join.join_strategy || "Unknown strategy")}</strong><span>${escapeHtml(join.join_type || "Type unavailable")}</span></header>${rows.length ? metricRows(rows) : `<div class="metric-unavailable">Operator metrics are unavailable in the Spark event log.</div>`}</article>`;
  };
  const preview = joins.slice(0, 2);
  const remaining = joins.slice(2);
  return `${metricRows(summaryRows)}<div class="operator-list">${preview.map(joinArticle).join("")}</div>${remaining.length ? `<details class="more-evidence"><summary>Show all ${joins.length} joins</summary><div class="operator-list">${remaining.map((join, index) => joinArticle(join, index + 2)).join("")}</div></details>` : ""}`;
}

function operatorDrilldown(block) {
  const metrics = block?.metrics || {};
  const flags = block?.problems || [];
  const rows = [];
  if (metrics.window_count) rows.push(["Largest Window input", fmtNumber(metrics.largest_window_input_rows)], ["Window upstream shuffle", fmtBytes(metrics.largest_window_upstream_shuffle_bytes)]);
  if (metrics.sort_count) rows.push(["Largest Sort input", fmtNumber(metrics.largest_sort_input_rows)]);
  if (metrics.exchange_count) rows.push(["Largest Exchange output size", fmtBytes(metrics.largest_exchange_bytes)]);
  if (metrics.single_partition_exchange_count) rows.push(["SinglePartition exchanges", fmtNumber(metrics.single_partition_exchange_count)]);
  if (metrics.max_row_expansion) rows.push(["Max Generate / Expand amplification", fmtRatio(metrics.max_row_expansion)]);
  rows.push(["Operators", `${fmtNumber(metrics.window_count)} Window · ${fmtNumber(metrics.sort_count)} Sort · ${fmtNumber(metrics.exchange_count)} Exchange`]);
  return `${metricRows(rows)}${flags.length ? `<div class="operator-flags">${flags.map(item => `<span class="${statusClass(item.severity)}">${escapeHtml(item.problem_code)}</span>`).join("")}</div>` : ""}`;
}

function diagnosticSummary(name, report) {
  const summary = report.summary_metrics || {}, block = report.diagnostics?.[name] || {}, metrics = block.metrics || {};
  if (name === "compute") return [`${fmtPct(summary.allocated_cpu_utilization_pct)} allocated CPU`, `${fmtPct(summary.task_slot_utilization_pct)} slot utilization`];
  if (name === "memory") return [`${fmtPct(summary.gc_ratio_pct)} GC`, `${fmtBytes(sumValues(summary.memory_spill_bytes, summary.disk_spill_bytes))} spill`];
  if (name === "skew") return [`${fmtRatio(summary.worst_duration_skew_factor)} duration skew`, `${fmtRatio(summary.worst_input_skew_factor)} input skew`];
  if (name === "shuffle") return [`${fmtBytes(summary.total_shuffle_io_bytes)} physical shuffle I/O`, `${fmtPct(summary.shuffle_fetch_wait_pct)} fetch wait`];
  if (name === "scan") return scanHeadline(block, summary);
  if (name === "joins") return metrics.join_count ? [plural(metrics.join_count, "join", "joins"), `${fmtRatio(metrics.max_join_amplification)} max amplification`] : ["No joins", "Not applicable"];
  return sqlHeadline(block);
}

function diagnosticCard(name, report, group) {
  const block = report.diagnostics?.[name] || {};
  const headline = diagnosticSummary(name, report);
  const drilldown = name === "scan" ? scanDrilldown(block) : name === "joins" ? joinDrilldown(block) : name === "sql_operators" ? operatorDrilldown(block) : metricRows(blockMetrics(name, report));
  const problemCode = block.problems?.[0]?.problem_code;
  return `<details class="diagnostic-panel ${group} diagnostic-${name} ${statusClass(block.status)}"><summary><div><strong>${escapeHtml(diagnosticNames[name])}</strong><span>${escapeHtml(problemCode || headline[0])} · ${escapeHtml(headline[1])}</span></div>${statusMark(block.status)}</summary><div class="diagnostic-detail">${problemList(block)}${drilldown}</div></details>`;
}

function contextPanels(report) {
  const summary = report.summary_metrics || {}, resources = report.resources || {};
  const longest = report.top_stages?.by_wall_duration?.[0];
  const coverage = report.coverage || {};
  const actualExecutorMin = resources.executor_count_min;
  const coverageCount = Object.values(coverage).filter(Boolean).length, coverageTotal = Object.keys(coverage).length;
  const coverageLabels = { task_metrics_available: "Task metrics", executor_cpu_time_available: "Executor CPU time", sql_events_available: "SQL events", sql_plan_available: "SQL plan", sql_metrics_available: "SQL metrics", aqe_events_available: "AQE events", executor_memory_metrics_available: "Executor memory metrics", application_completed: "Application completed", log_complete: "Event log complete" };
  const stageSignal = longest ? diagnosticOrder.flatMap(name => report.diagnostics?.[name]?.problems || []).find(problem => (problem.affected_stages || []).includes(longest.stage_id)) : null;
  const driverOverhead = configValue(report, "spark.driver.memoryOverhead");
  const broadcastThreshold = configValue(report, "spark.sql.adaptive.autoBroadcastJoinThreshold") !== "—"
    ? configValue(report, "spark.sql.adaptive.autoBroadcastJoinThreshold") : configValue(report, "spark.sql.autoBroadcastJoinThreshold");
  return `<section class="execution-context" aria-label="Spark run setup">
      <div class="context-heading"><strong>Run setup</strong><span>configuration</span></div>
      <div class="context-cell"><span>Driver</span><strong>${summary.driver_cores_config ?? "—"} cores · ${summary.driver_memory_config ?? "—"}${driverOverhead !== "—" ? ` · ${driverOverhead} OH` : ""}</strong></div>
      <div class="context-cell"><span>Per executor</span><strong>${summary.executor_cores_config ?? "—"} cores · ${summary.executor_memory_config ?? "—"} · ${summary.executor_memory_overhead_config ?? "—"} OH</strong></div>
      <div class="context-cell executor-setup"><span>Executor count</span><div class="context-pairs"><div><b>Configured</b><small>min / initial / max</small><strong>${summary.min_executors ?? "—"} / ${summary.initial_executors ?? "—"} / ${summary.max_executors ?? "—"}</strong></div><div><b>Actual</b><small>min / avg / peak</small><strong>${fmtNumber(actualExecutorMin)} / ${fmtNumber(summary.actual_average_executors)} / ${fmtNumber(summary.actual_peak_executors)}</strong></div></div></div>
      <div class="context-cell"><span>Spark SQL</span><strong>AQE ${configFlag(report, "spark.sql.adaptive.enabled")} · skew ${configFlag(report, "spark.sql.adaptive.skewJoin.enabled")} · shuffle ${configValue(report, "spark.sql.shuffle.partitions")} · broadcast ${broadcastThreshold}</strong></div>
      <details class="context-cell context-detail"><summary><span>Longest stage</span><strong>${longest ? `#${longest.stage_id} · ${fmtDuration(longest.wall_duration_ms)} · ${fmtPct(longest.stage_wall_share_pct)}` : "N/A"}</strong></summary>${longest ? `<div class="context-popover"><strong class="popover-title">${escapeHtml(longest.name)}</strong>${metricRows([...(stageSignal ? [["Main signal", stageSignal.problem_code]] : []), ["Tasks", fmtNumber(longest.task_count)], ["Input", fmtBytes(longest.input_bytes)], ["Physical shuffle I/O", fmtBytes(sumValues(longest.shuffle_read_bytes, longest.shuffle_write_bytes))]])}</div>` : ""}</details>
      <details class="context-cell context-detail coverage-cell"><summary><span>Coverage</span><strong>${coverageCount} / ${coverageTotal} ${coverageCount === coverageTotal ? "✓" : "⚠"}</strong></summary><div class="context-popover coverage-popover">${Object.entries(coverage).map(([key, value]) => `<span class="${value ? "yes" : "no"}">${value ? "✓" : "×"} ${escapeHtml(coverageLabels[key] || key.replaceAll("_", " "))}</span>`).join("")}</div></details>
    </section>`;
}

function detailContent(report) {
  return `${contextPanels(report)}<div class="diagnostics-line" aria-label="Seven Spark diagnostics">${diagnosticOrder.map(name => diagnosticCard(name, report, "inline")).join("")}</div>`;
}

async function toggleDetails(row) {
  const reportId = row.dataset.report, detailRow = document.querySelector(`[data-details="${reportId}"]`);
  if (!detailRow) return;
  const shouldOpen = detailRow.hidden;
  document.querySelectorAll(".details-row").forEach(item => { if (item !== detailRow) item.hidden = true; });
  document.querySelectorAll(".app-row").forEach(item => { if (item !== row) item.setAttribute("aria-expanded", "false"); });
  detailRow.hidden = !shouldOpen; row.setAttribute("aria-expanded", String(shouldOpen));
  if (!shouldOpen || detailRow.dataset.loaded) return;
  try {
    const response = await fetch(`/api/reports/${encodeURIComponent(reportId)}`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const report = await response.json();
    detailRow.querySelector(".details").innerHTML = detailContent(report); detailRow.dataset.loaded = "true";
    detailRow.querySelectorAll(".context-detail").forEach(panel => panel.addEventListener("toggle", () => {
      if (!panel.open) return;
      detailRow.querySelectorAll(".context-detail[open]").forEach(other => { if (other !== panel) other.open = false; });
    }));
  } catch (error) {
    detailRow.querySelector(".details").innerHTML = `<div class="empty">Не удалось загрузить детали: ${escapeHtml(error.message)}</div>`;
  }
}

function bindRows() {
  document.querySelectorAll(".app-row").forEach(row => {
    row.addEventListener("click", () => toggleDetails(row));
    row.addEventListener("keydown", event => { if (["Enter", " "].includes(event.key)) { event.preventDefault(); toggleDetails(row); } });
  });
}

function showReports(reports, context = {}) {
  if (reports.length) {
    $("#job-panel").hidden = true;
    $("#job-errors").textContent = "";
    setFormBusy(false);
  }
  $("#app-rows").innerHTML = reports.length ? reports.map(applicationRow).join("") : `<tr><td colspan="8" class="empty">Введите DAG и запустите расчёт.</td></tr>`;
  $("#app-count").textContent = fmtNumber(reports.length); $("#visible-count").textContent = `${reports.length} строк`;
  const severities = reports.map(report => {
    const statuses = Object.values(report.diagnostics || {}).map(block => block?.status);
    if (statuses.includes("CRITICAL")) return "CRITICAL";
    if (statuses.includes("WARNING")) return "WARNING";
    if (statuses.includes("NOT_AVAILABLE")) return "UNKNOWN";
    return "OK";
  });
  const severityCount = status => severities.filter(value => value === status).length;
  const attention = severityCount("CRITICAL") + severityCount("WARNING");
  $("#problem-count").textContent = `${attention} из ${reports.length}`;
  $("#status-summary").innerHTML = `<i class="dot critical"></i>${severityCount("CRITICAL")} critical <i class="dot warning"></i>${severityCount("WARNING")} warning <i class="dot"></i>${severityCount("UNKNOWN")} unknown <i class="dot ok"></i>${severityCount("OK")} ok`;
  $("#run-dag").textContent = context.dag_name || (reports[0]?.application?.application_name || "—").split("__")[0];
  $("#run-window").textContent = context.run_start ? `${fmtTime(context.run_start)} — ${fmtTime(context.run_end)}` : (reports[0] ? fmtTime(reports[0].application?.start_time) : "—");
  const dagInput = $("#dag-name");
  if (context.dag_name && (document.activeElement !== dagInput || !dagInput.value.trim())) dagInput.value = context.dag_name;
  bindRows();
}

function showPendingDag(dagName) {
  $("#app-rows").innerHTML = `<tr><td colspan="8" class="empty">Ищу приложения и рассчитываю метрики для <strong>${escapeHtml(dagName)}</strong>…</td></tr>`;
  $("#run-dag").textContent = dagName;
  $("#run-window").textContent = "—";
  $("#app-count").textContent = "—";
  $("#problem-count").textContent = "—";
  $("#visible-count").textContent = "расчёт";
}

function setFormBusy(busy) {
  $("#analyze-button").disabled = busy;
  $("#dag-name").disabled = busy;
}

async function loadReports() {
  const response = await fetch("/api/reports"); if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const payload = await response.json(); showReports(payload.reports || [], payload.context || {});
}

function updateJob(job) {
  $("#job-message").textContent = job.message || "Выполняется…"; $("#job-counter").textContent = job.total ? `${job.completed} / ${job.total}` : "поиск";
  $("#job-progress").style.width = `${job.total ? Math.round(job.completed / job.total * 100) : (job.phase === "DISCOVERY" ? 8 : 0)}%`;
  $("#job-errors").textContent = (job.errors || []).join(" · ");
}

async function pollJob(jobId) {
  const response = await fetch(`/api/jobs/${encodeURIComponent(jobId)}`); if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const job = await response.json(); updateJob(job);
  if (job.status === "COMPLETED") { setFormBusy(false); await loadReports(); return; }
  if (job.status === "FAILED") { setFormBusy(false); await loadReports(); throw new Error(job.message || "Расчёт завершился с ошибкой"); }
  window.setTimeout(() => pollJob(jobId).catch(showJobError), 900);
}

function showJobError(error) {
  setFormBusy(false); $("#job-message").textContent = "Не удалось выполнить расчёт"; $("#job-errors").textContent = error.message;
}

$("#analyze-form").addEventListener("submit", async event => {
  event.preventDefault(); const dagName = $("#dag-name").value.trim(); if (!dagName) return;
  setFormBusy(true); showPendingDag(dagName); $("#job-panel").hidden = false; $("#job-message").textContent = "Ищу приложения последнего запуска…";
  $("#job-counter").textContent = "поиск"; $("#job-progress").style.width = "8%"; $("#job-errors").textContent = "";
  try {
    const response = await fetch("/api/analyze", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ dag_name: dagName }) });
    const payload = await response.json(); if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`); await pollJob(payload.job_id);
  } catch (error) { showJobError(error); }
});

loadReports().catch(error => { $("#app-rows").innerHTML = `<tr><td colspan="8" class="empty">Не удалось загрузить отчёт: ${escapeHtml(error.message)}</td></tr>`; });
