import fs from "node:fs";
import path from "node:path";

const YEARS = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023];
const LEGACY_RATES = {
  hi: 0.4,
  le: 0.08,
  as: -0.6,
  la: -0.5,
  va: -0.2,
  pv: -0.3,
  un: -0.25,
  hs: 0.15,
  fd: 0.3,
  gs: 0.1,
  hw: -0.2,
  cr: -0.5,
  in: 0.8,
  tp: 0.2,
  dp: 0.4,
  rt: 0.25,
  ws: 0.3,
  hz: 0.45,
};
const BENCHMARK_METRICS = new Set(["hi", "le", "as", "la", "va", "pv", "un", "hs", "fd", "gs", "cr"]);
const INVERSE_METRICS = new Set(["as", "la", "va", "pv", "un", "cr"]);

function round1(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return null;
  return Math.round(Number(value) * 10) / 10;
}

function legacySeries(value, metric) {
  if (Array.isArray(value)) {
    return value.map((entry) => round1(entry));
  }
  if (value === null || value === undefined) {
    return YEARS.map(() => null);
  }
  const base = Number(value);
  const rate = LEGACY_RATES[metric] ?? 0;
  return YEARS.map((_, yi) => round1(base + rate * (yi - 7) + Math.sin(yi * 17 + base) * 0.25));
}

function mean(values) {
  const usable = values.filter((value) => value !== null && value !== undefined && Number.isFinite(value));
  if (!usable.length) return null;
  return round1(usable.reduce((sum, value) => sum + value, 0) / usable.length);
}

function deriveBenchmarkRecord(cityRecord, level) {
  const result = {};
  const multiplier = level === "state" ? 1 : 2;

  for (const metric of BENCHMARK_METRICS) {
    if (!(metric in cityRecord)) continue;
    const cityValue = cityRecord[metric];
    if (cityValue === null || cityValue === undefined) continue;
    const delta = INVERSE_METRICS.has(metric) ? -0.8 * multiplier : 0.8 * multiplier;
    result[metric] = round1(cityValue + delta);
  }

  return result;
}

function normalizeLegacyDataset(raw) {
  const neighborhoods = {};

  for (const [name, metrics] of Object.entries(raw)) {
    const yearly = {};

    for (const year of YEARS) {
      yearly[String(year)] = {};
    }

    for (const [metric, value] of Object.entries(metrics ?? {})) {
      const series = legacySeries(value, metric);
      YEARS.forEach((year, index) => {
        const entry = series[index];
        if (entry !== null) yearly[String(year)][metric] = entry;
      });
    }

    neighborhoods[name] = yearly;
  }

  const benchmarks = { city: {}, state: {}, federal: {} };

  for (const year of YEARS) {
    const yearKey = String(year);
    const metricSet = new Set();

    for (const record of Object.values(neighborhoods)) {
      for (const metric of Object.keys(record[yearKey] ?? {})) {
        metricSet.add(metric);
      }
    }

    const cityRecord = {};
    for (const metric of metricSet) {
      cityRecord[metric] = mean(
        Object.values(neighborhoods).map((record) => record[yearKey]?.[metric] ?? null),
      );
    }

    benchmarks.city[yearKey] = cityRecord;
    benchmarks.state[yearKey] = deriveBenchmarkRecord(cityRecord, "state");
    benchmarks.federal[yearKey] = deriveBenchmarkRecord(cityRecord, "federal");
  }

  return {
    meta: {
      schema_version: 2,
      years: YEARS,
      provisional: true,
      note: "Neighborhood yearly records remain modeled approximations unless supplied as real arrays by the pipeline.",
      benchmark_note: "City benchmarks are derived from Baltimore CSA values. State and federal benchmarks are provisional scaffolds until ACS/FRED imports are connected.",
    },
    neighborhoods,
    benchmarks,
  };
}

const argPath = process.argv[2] ?? "data.json";
const dataPath = path.resolve(process.cwd(), argPath);
const raw = JSON.parse(fs.readFileSync(dataPath, "utf8"));

if (raw?.neighborhoods && raw?.benchmarks) {
  console.log(`Already normalized: ${dataPath}`);
  process.exit(0);
}

const migrated = normalizeLegacyDataset(raw);
fs.writeFileSync(dataPath, JSON.stringify(migrated, null, 2) + "\n");
console.log(`Migrated legacy dashboard data -> ${dataPath}`);
