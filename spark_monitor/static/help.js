const docsNumber = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 1 });
const docsInteger = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 });

function nestedValue(object, path) {
  return path.split(".").reduce((value, key) => value?.[key], object);
}

function formatBytes(value) {
  if (!Number.isFinite(Number(value))) return "—";
  let amount = Number(value), unit = 0;
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  while (Math.abs(amount) >= 1024 && unit < units.length - 1) {
    amount /= 1024;
    unit += 1;
  }
  return `${docsNumber.format(amount)} ${units[unit]}`;
}

function formatDuration(value) {
  if (!Number.isFinite(Number(value))) return "—";
  const seconds = Number(value) / 1000;
  if (seconds >= 60) return `${docsNumber.format(seconds / 60)} мин`;
  return `${docsNumber.format(seconds)} с`;
}

function formatRule(value, format) {
  if (value === null || value === undefined) return "—";
  switch (format) {
    case "pct": return `${docsNumber.format(Number(value))}%`;
    case "ratio": return docsNumber.format(Number(value));
    case "times": return `${docsNumber.format(Number(value))}×`;
    case "bytes": return formatBytes(value);
    case "duration": return formatDuration(value);
    case "core-seconds": return `${docsNumber.format(Number(value))} core-s`;
    case "rows": return `${docsInteger.format(Number(value))} rows`;
    case "integer": return docsInteger.format(Number(value));
    default: return String(value);
  }
}

async function loadDocumentationRules() {
  const sourceBox = document.querySelector(".rules-source");
  const source = document.querySelector("#rules-source-value");
  const footer = document.querySelector("#rules-source-footer");
  const state = document.querySelector("#rules-load-state");

  try {
    const response = await fetch("/api/rules", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const payload = await response.json();
    const rules = payload.rules || {};
    const rulesVersion = rules.rules_version ?? "—";
    const rulesSource = payload.source || "backend rules config";

    document.querySelectorAll("[data-rule]").forEach(element => {
      const value = nestedValue(rules, element.dataset.rule);
      element.textContent = formatRule(value, element.dataset.format);
      if (value === null || value === undefined) element.title = `В конфигурации нет ${element.dataset.rule}`;
    });

    source.textContent = `rules v${rulesVersion} · ${rulesSource}`;
    footer.textContent = rulesSource;
    state.textContent = "Значения синхронизированы с текущим расчётом";
  } catch (error) {
    sourceBox?.classList.add("error");
    source.textContent = "Не удалось загрузить активные границы";
    footer.textContent = "backend rules config (недоступен)";
    state.textContent = "Обновите страницу или проверьте backend";
  }
}

loadDocumentationRules();
