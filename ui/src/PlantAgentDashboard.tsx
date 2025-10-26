import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend, CartesianGrid } from "recharts";

/**
 * Plant Agent – Operations Dashboard UI (BQ-aware)
 * Align trends/snapshot sources; add dwell; remove “auto” direction; keep last-runs fresh.
 * (Update: clean trends to avoid 10 tph resets; prefer BQ; use live countdown in header chip)
 */

type Snapshot = {
  production_tph: number;
  kiln_feed_tph: number;
  separator_dp_pa: number;
  id_fan_flow_Nm3_h: number;
  cooler_airflow_Nm3_h: number;
  kiln_speed_rpm: number;
  o2_percent: number;
  specific_power_kwh_per_ton: number;
};

type TrendPoint = {
  t: string;
  production_tph: number;
  o2_percent: number;
  specific_power_kwh_per_ton: number;
};

type RoutineReq = {
  snapshot?: Snapshot;
  targets?: Record<string, any>;
  constraints?: Record<string, any>;
  apply_top?: boolean;
  log_suggestions?: boolean;
};

type RoutineResp = {
  mode: string;
  current: Snapshot;
  predicted_after?: any;
  proposed_setpoints?: Record<string, number>;
  bq_log?: { table?: string; insert_error?: string | null };
  suggestion_id?: string;
  created_at?: string;
  applied?: boolean;
  actuation?: { after?: Snapshot } | null;
};

type Stage = { name?: string; checks?: string[]; setpoints: Record<string, number> };

type LoadReq = {
  snapshot?: Snapshot;
  direction?: "up" | "down";
  delta_pct?: number | null;
  delta_abs?: number | null;
  target_tph?: number | null;
  steps?: number | null;
  constraints?: Record<string, any>;
};

type LoadResp = {
  plan_id: string;
  created_at: string;
  mode: string; // load_up | load_down
  current: Snapshot;
  predicted_after?: any;
  actions?: any;
  stages: Stage[];
  target: any;
  bq_log?: { table?: string; insert_error?: string | null };
};

type ApplyResp = {
  ok: boolean;
  bq_log?: { table?: string; insert_error?: string | null };
  before?: Partial<Snapshot> | null;
  after?: Partial<Snapshot> | null;
  applied_at?: string;
};

type LastRuns = {
  last_cron_routine: string | null;
  last_ingest: string | null;
  sched_period_sec: number;
  now: string;
  next_cron_eta: string | null;
  seconds_to_next: number | null;
};

const LS_KEYS = {
  BASE: "plant_ui.base_url",
  TOKEN: "plant_ui.id_token",
  AUTOPOLL: "plant_ui.autopoll",
  STEP_DWELL: "plant_ui.step_dwell",
};

function useLocalStorage(key: string, initial: string) {
  const [v, setV] = useState<string>(() => localStorage.getItem(key) ?? initial);
  useEffect(() => { localStorage.setItem(key, v); }, [key, v]);
  return [v, setV] as const;
}

function cls(...xs: (string | false | undefined | null)[]) { return xs.filter(Boolean).join(" "); }

function fmt(n: number | undefined | null, d = 3) {
  if (n === undefined || n === null || Number.isNaN(n)) return "-";
  return Number(n).toFixed(d);
}

function safeNum(n: any): number | undefined { const x = Number(n); return Number.isFinite(x) ? x : undefined; }

function normalizeBase(u: string) {
  if (!u) return "";
  let s = u.trim();
  s = s.replace(/\/+$/, "");
  s = s.replace(/\/snapshot$/i, "");
  return s;
}

function formatSuggestionText(
  lever: string, current: number | undefined, proposed: number | undefined, deltaPct: number | undefined, cmin?: number, cmax?: number
) {
  let base: string;
  if (deltaPct === undefined || current === undefined || current === 0 || proposed === undefined) {
    base = `${lever}: set to ${fmt(proposed)}`;
  } else if (deltaPct > 0) {
    base = `Increase ${lever} by ~${Math.abs(Number(deltaPct.toFixed(1)))}% to ${fmt(proposed)}`;
  } else if (deltaPct < 0) {
    base = `Reduce ${lever} by ~${Math.abs(Number(deltaPct.toFixed(1)))}% to ${fmt(proposed)}`;
  } else {
    base = `Hold ${lever} at ${fmt(proposed)}`;
  }
  const b = (cmin !== undefined || cmax !== undefined)
    ? ` (bounds ${cmin !== undefined ? fmt(cmin) : "−∞"}…${cmax !== undefined ? fmt(cmax) : "+∞"})` : "";
  return base + b;
}

function deriveSuggestionLines(current: Snapshot | null, proposed?: Record<string, number>, constraints?: Record<string, any>) {
  if (!current || !proposed) return [] as string[];
  const out: string[] = [];
  for (const [lever, p] of Object.entries(proposed)) {
    const cur = safeNum((current as any)[lever]);
    const prop = safeNum(p);
    const deltaAbs = cur !== undefined && prop !== undefined ? prop - cur : undefined;
    const deltaPct = cur && cur !== 0 && prop !== undefined ? ((prop - cur) / cur) * 100 : undefined;
    const cMin = constraints?.[lever]?.min;
    const cMax = constraints?.[lever]?.max;
    out.push(
      formatSuggestionText(lever, cur, prop, deltaPct, cMin, cMax) +
        (deltaAbs !== undefined ? ` (Δabs ${deltaAbs >= 0 ? "+" : ""}${Number(deltaAbs.toFixed(3))})` : "")
    );
  }
  return out;
}

function Chip({ children, tone="slate", }: { children: React.ReactNode; tone?: "slate" | "green" | "rose" | "amber" | "indigo"; }) {
  const map: Record<string, string> = {
    slate: "bg-slate-100 text-slate-700",
    green: "bg-green-100 text-green-700",
    rose: "bg-rose-100 text-rose-700",
    amber: "bg-amber-100 text-amber-800",
    indigo: "bg-indigo-100 text-indigo-700",
  };
  return <span className={cls("px-2 py-1 rounded-full border text-xs", map[tone])}>{children}</span>;
}

/** Measure box */
function useMeasure() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({ w: 0, h: 0 });
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) setSize({ w: Math.floor(e.contentRect.width), h: Math.floor(e.contentRect.height) });
    });
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return { ref, ...size };
}

async function tryFetchJSON(base: string, headers: Record<string, string>, paths: string[]) {
  let lastErr: any;
  for (const p of paths) {
    try {
      const r = await fetch(`${base}${p}`, { headers });
      if (r.ok) return { response: r, json: await r.json() };
      if (r.status !== 404) throw new Error(`${p} ${r.status}`);
      lastErr = new Error(`${p} 404`);
    } catch (e) { lastErr = e; }
  }
  throw lastErr ?? new Error("No candidate path succeeded");
}

/** Clean/denoise trend rows to avoid “snap-to-10” artifacts */
function cleanTrends(rows: any[]): TrendPoint[] {
  const sorted = [...rows].sort((a, b) => new Date(a.ts ?? 0).getTime() - new Date(b.ts ?? 0).getTime());
  const out: TrendPoint[] = [];
  let lastProd: number | undefined;

  for (const row of sorted) {
    const prod = Number(row.production_tph);
    const o2 = Number(row.o2_percent);
    const sp = Number(row.specific_power_kwh_per_ton);

    const prodGood = Number.isFinite(prod) && prod > 0.5 && prod < 1000;
    const o2Good = Number.isFinite(o2) && o2 >= 0 && o2 < 30;
    const spGood = Number.isFinite(sp) && sp > 0 && sp < 200;

    const spike = lastProd && prodGood ? Math.abs(prod - lastProd) / lastProd > 0.30 : false;
    const keepProd = prodGood && !spike ? prod : NaN;
    if (Number.isFinite(keepProd)) lastProd = keepProd as number;

    out.push({
      t: row.ts ? new Date(row.ts).toLocaleTimeString() : "",
      production_tph: Number.isFinite(keepProd) ? (keepProd as number) : (NaN as any),
      o2_percent: o2Good ? o2 : (NaN as any),
      specific_power_kwh_per_ton: spGood ? sp : (NaN as any),
    });
  }
  return out;
}

export default function PlantAgentDashboard() {
  const [baseRaw, setBaseRaw] = useLocalStorage(LS_KEYS.BASE, "");
  const base = useMemo(() => normalizeBase(baseRaw), [baseRaw]);
  const [token, setToken] = useLocalStorage(LS_KEYS.TOKEN, "");
  const [autoPoll, setAutoPoll] = useLocalStorage(LS_KEYS.AUTOPOLL, "1");
  const [stepDwellCsv, setStepDwellCsv] = useLocalStorage(LS_KEYS.STEP_DWELL, "20");

  const headers = useMemo(() => {
    const h: Record<string, string> = { "Content-Type": "application/json" };
    if (token.trim()) h["Authorization"] = `Bearer ${token.trim()}`;
    return h;
  }, [token]);

  const [health, setHealth] = useState<string>("");
  const [ver, setVer] = useState<string>("");
  const [errorMsg, setErrorMsg] = useState<string>("");

  const [lastRuns, setLastRuns] = useState<LastRuns | null>(null);
  const [countdown, setCountdown] = useState<number | null>(null);

  const fetchHealth = useCallback(async () => {
    setErrorMsg("");
    try {
      const healthCandidates = [`/health`, `/snapshot/health`, `/healthz`];
      let healthResp: Response | null = null;
      for (const p of healthCandidates) {
        try {
          const rr = await fetch(`${base}${p}`, { headers });
          healthResp = rr;
          if (rr.ok || rr.status !== 404) break;
        } catch {}
      }
      if (!healthResp) throw new Error("No response");
      setHealth(`${healthResp.status}`);

      const { json: verJson } = await tryFetchJSON(base, headers, [`/version`, `/snapshot/version`]);
      setVer(verJson.version ?? "");
    } catch (e: any) {
      setHealth("error");
      setErrorMsg(e?.message || "Failed to reach /health");
    }
  }, [base, headers]);

  // Snapshot + trends
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [history, setHistory] = useState<TrendPoint[]>([]);
  const [trends, setTrends] = useState<TrendPoint[]>([]);
  const pollRef = useRef<number | null>(null);

  const pushHistory = useCallback((s: Snapshot, tLabel?: string) => {
    setHistory((prev) => [
      ...prev.slice(-240),
      { t: tLabel ?? new Date().toLocaleTimeString(), production_tph: s.production_tph, o2_percent: s.o2_percent, specific_power_kwh_per_ton: s.specific_power_kwh_per_ton },
    ]);
  }, []);

  const fetchSnapshotFast = useCallback(async (): Promise<Snapshot | null> => {
    if (!base) return null;
    const urls = [`${base}/snapshot`, `${base}/snapshot?source=bq`];
    for (const u of urls) {
      try {
        const r = await fetch(u, { headers });
        if (!r.ok) continue;
        const j = (await r.json()) as Snapshot;
        setSnap(j);
        pushHistory(j);
        return j;
      } catch {}
    }
    return null;
  }, [base, headers, pushHistory]);

  const fetchSnapshot = useCallback(async () => {
    if (!base) return;
    const s = await fetchSnapshotFast();
    if (!s) setErrorMsg("Failed to fetch snapshot");
  }, [base, fetchSnapshotFast]);

  const fetchTrends = useCallback(async () => {
    if (!base) return;
    setErrorMsg("");
    try {
      // Prefer BQ to avoid mixed-source artifacts; fallback to auto only if BQ missing.
      const { json: data } = await tryFetchJSON(base, headers, [
        `/trends?minutes=120&limit=240&source=bq`,
        `/trends?minutes=120&limit=240&source=auto`,
      ]);
      if (Array.isArray(data)) {
        const cleaned = cleanTrends(data);
        setTrends(cleaned.slice(-240));
      }
    } catch (e: any) {
      setErrorMsg((prev) => prev || e?.message || "Failed to fetch trends");
    }
  }, [base, headers]);

  // NEW: read last_runs and maintain countdown
  const fetchLastRuns = useCallback(async () => {
    if (!base) return;
    try {
      const r = await fetch(`${base}/debug/last_runs`, { headers });
      if (!r.ok) return;
      const j = (await r.json()) as LastRuns;
      setLastRuns(j);
      setCountdown(j.seconds_to_next ?? null);
    } catch {}
  }, [base, headers]);

  useEffect(() => {
    if (!base) return;
    if (/\/snapshot\/?$/i.test(baseRaw)) {
      setErrorMsg((m) => m || "Tip: remove '/snapshot' from the API Base URL; the app adds paths itself.");
    }
    fetchHealth();
    fetchTrends();
    fetchSnapshot();
    fetchLastRuns();
  }, [base]); // eslint-disable-line

  useEffect(() => {
    const enabled = autoPoll === "1";
    if (!enabled || !base) {
      if (pollRef.current) window.clearInterval(pollRef.current);
      pollRef.current = null;
      return;
    }
    pollRef.current = window.setInterval(() => {
      fetchSnapshot().catch(() => {});
      fetchLastRuns().catch(() => {});
    }, 5000);
    return () => {
      if (pollRef.current) window.clearInterval(pollRef.current);
      pollRef.current = null;
    };
  }, [autoPoll, base, fetchSnapshot, fetchLastRuns]);

  // live countdown tick
  useEffect(() => {
    if (countdown === null) return;
    const id = window.setInterval(() => { setCountdown((c) => (c === null ? c : Math.max(0, c - 1))); }, 1000);
    return () => window.clearInterval(id);
  }, [countdown]);

  // Metrics (still available via button)
  const [metrics, setMetrics] = useState<any>(null);
  const getMetrics = useCallback(async () => {
    setErrorMsg("");
    try {
      const { json } = await tryFetchJSON(base, headers, [`/metrics`, `/snapshot/metrics`]);
      setMetrics(json);
    } catch (e: any) { setErrorMsg(e?.message || "Failed to fetch metrics"); }
  }, [base, headers]);

  // Routine
  const [o2Min, setO2Min] = useState<string>("2.3");
  const [o2Max, setO2Max] = useState<string>("4.5");
  const [applyTop, setApplyTop] = useState<boolean>(false);
  const [logSugg, setLogSugg] = useState<boolean>(true);
  const [routineOut, setRoutineOut] = useState<RoutineResp | null>(null);

  const [routineBefore, setRoutineBefore] = useState<Snapshot | null>(null);
  const [routineAfter, setRoutineAfter] = useState<Snapshot | null>(null);

  const runRoutine = useCallback(async () => {
    setErrorMsg("");
    try {
      const body: RoutineReq = {
        constraints: { o2_percent: { min: Number(o2Min) || undefined, max: Number(o2Max) || undefined } },
        apply_top: applyTop,
        log_suggestions: logSugg,
      };

      // Use current in-memory snapshot as "before"
      const s0 = snap ?? (await fetchSnapshotFast());
      if (s0) setRoutineBefore({ ...s0 });

      const r = await fetch(`${base}/optimize/routine`, { method: "POST", headers, body: JSON.stringify(body) });
      if (!r.ok) throw new Error(`/optimize/routine ${r.status}`);
      const j: RoutineResp = await r.json();
      setRoutineOut(j);

      // refresh last-runs immediately (backend increments on manual run now)
      fetchLastRuns().catch(() => {});

      // Only update snapshot if backend applied (apply_top true)
      if (j.applied && j.actuation?.after) {
        const after = j.actuation.after as Snapshot;
        setSnap(after);
        pushHistory(after);
        setRoutineAfter({ ...after });
        await fetchTrends();
      }
    } catch (e: any) {
      setErrorMsg(e?.message || "Run routine failed");
    }
  }, [base, headers, o2Min, o2Max, applyTop, logSugg, snap, fetchSnapshotFast, pushHistory, fetchTrends, fetchLastRuns]);

  const applyRoutineProposal = useCallback(async () => {
    if (!routineOut?.proposed_setpoints) return;
    setErrorMsg("");
    try {
      const body = { proposal: routineOut.proposed_setpoints, mode: "routine" };
      const r = await fetch(`${base}/actuate/apply_stage`, { method: "POST", headers, body: JSON.stringify(body) });
      if (!r.ok) throw new Error(`/actuate/apply_stage ${r.status}`);
      const j: ApplyResp = await r.json();

      let after: Snapshot | null = j.after ? (j.after as Snapshot) : await fetchSnapshotFast();
      if (after) {
        setSnap(after);
        pushHistory(after);
        setRoutineAfter({ ...after });
        await fetchTrends();
      }
      // ingest time may have changed because of auto-ingest; update last-runs
      fetchLastRuns().catch(() => {});
    } catch (e: any) {
      setErrorMsg(e?.message || "Apply failed");
    }
  }, [base, headers, routineOut, fetchSnapshotFast, pushHistory, fetchTrends, fetchLastRuns]);

  const rejectRoutine = useCallback(() => {
    setRoutineOut((x) => (x ? { ...x, applied: false } : x));
    setRoutineBefore(null);
    setRoutineAfter(null);
  }, []);

  const suggestionLines = useMemo(
    () => deriveSuggestionLines(snap, routineOut?.proposed_setpoints, { o2_percent: { min: Number(o2Min), max: Number(o2Max) } }),
    [snap, routineOut, o2Min, o2Max]
  );

  // Load planning
  const [loadMode, setLoadMode] = useState<"pct" | "abs" | "target">("pct");
  const [steps, setSteps] = useState<string>("3");
  const [direction, setDirection] = useState<"up" | "down">("up");
  const [val, setVal] = useState<string>("8");
  const [loadOut, setLoadOut] = useState<LoadResp | null>(null);

  const [loadBefore, setLoadBefore] = useState<Snapshot | null>(null);
  const [loadAfter, setLoadAfter] = useState<Snapshot | null>(null);

  const runLoad = useCallback(async () => {
    setErrorMsg("");
    try {
      const body: LoadReq = { steps: Number(steps) || 3, direction } as any;
      if (loadMode === "pct") body.delta_pct = Number(val);
      if (loadMode === "abs") body.delta_abs = Number(val);
      if (loadMode === "target") body.target_tph = Number(val);

      const r = await fetch(`${base}/optimize/load`, { method: "POST", headers, body: JSON.stringify(body) });
      if (!r.ok) throw new Error(`/optimize/load ${r.status}`);
      const j: LoadResp = await r.json();
      setLoadOut(j);

      const s0 = snap ?? (await fetchSnapshotFast());
      if (s0) setLoadBefore({ ...s0 });
      setLoadAfter(null);
    } catch (e: any) {
      setErrorMsg(e?.message || "Create plan failed");
    }
  }, [base, headers, steps, direction, val, loadMode, fetchSnapshotFast, snap]);

  // Helper to parse per-step dwell seconds (comma separated)
  const parseStepDwells = useCallback((nStages: number): number[] => {
    const parts = stepDwellCsv.split(",").map((s) => Number(s.trim())).filter((x) => Number.isFinite(x) && x >= 0);
    if (parts.length === 0) return Array(nStages).fill(20);
    const out: number[] = [];
    for (let i = 0; i < nStages; i++) out.push(parts[i] ?? parts[0]);
    return out;
  }, [stepDwellCsv]);

  const applyStage = useCallback(async (i: number) => {
    if (!loadOut) return;
    setErrorMsg("");
    try {
      const body = { stage: loadOut.stages[i], mode: loadOut.mode, plan_id: loadOut.plan_id, stage_index: i };
      const r = await fetch(`${base}/actuate/apply_stage`, { method: "POST", headers, body: JSON.stringify(body) });
      if (!r.ok) throw new Error(`/actuate/apply_stage ${r.status}`);
      const j: ApplyResp = await r.json();

      let after: Snapshot | null = j.after ? (j.after as Snapshot) : await fetchSnapshotFast();
      if (after) {
        setSnap(after);
        pushHistory(after);
        if (i === loadOut.stages.length - 1) setLoadAfter({ ...after });
        setRoutineBefore(null);
        setRoutineAfter(null);
        await fetchTrends();
      }
      fetchLastRuns().catch(() => {});
    } catch (e: any) {
      setErrorMsg(e?.message || `Apply stage ${i + 1} failed`);
    }
  }, [base, headers, loadOut, fetchSnapshotFast, pushHistory, fetchTrends, fetchLastRuns]);

  const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

  const applyAllStages = useCallback(async () => {
    if (!loadOut) return;
    if (!loadBefore) {
      const s0 = snap ?? (await fetchSnapshotFast());
      if (s0) setLoadBefore({ ...s0 });
    }
    const dwells = parseStepDwells(loadOut.stages.length);
    for (let i = 0; i < loadOut.stages.length; i++) {
      await applyStage(i);
      await sleep(Math.max(0, (dwells[i] ?? 0) * 1000));
    }
    if (!loadAfter) {
      const s1 = await fetchSnapshotFast();
      if (s1) setLoadAfter({ ...s1 });
    }
  }, [loadOut, applyStage, loadBefore, fetchSnapshotFast, snap, loadAfter, parseStepDwells]);

  const acceptPlan = useCallback(async () => { if (loadOut) await applyAllStages(); }, [loadOut, applyAllStages]);

  const rejectPlan = useCallback(() => { setLoadOut(null); setLoadBefore(null); setLoadAfter(null); }, []);

  const disabled = !base;
  const kpi = snap;
  const chartData: TrendPoint[] = trends.length ? trends : history;

  const finalDeltaLoad = useMemo(() => {
    if (!loadBefore || !loadAfter) return null;
    const keys: (keyof Snapshot)[] = [
      "production_tph","o2_percent","specific_power_kwh_per_ton","kiln_feed_tph","separator_dp_pa","id_fan_flow_Nm3_h","cooler_airflow_Nm3_h","kiln_speed_rpm",
    ];
    return keys.map((k) => {
      const b = Number((loadBefore as any)[k]);
      const a = Number((loadAfter as any)[k]);
      const d = Number.isFinite(b) && Number.isFinite(a) ? a - b : undefined;
      const p = Number.isFinite(b) && b !== 0 && Number.isFinite(a) ? ((a - b) / b) * 100 : undefined;
      return { k, before: b, after: a, delta: d, pct: p };
    });
  }, [loadBefore, loadAfter]);

  const finalDeltaRoutine = useMemo(() => {
    if (!routineBefore || !routineAfter) return null;
    const keys: (keyof Snapshot)[] = [
      "production_tph","o2_percent","specific_power_kwh_per_ton","kiln_feed_tph","separator_dp_pa","id_fan_flow_Nm3_h","cooler_airflow_Nm3_h","kiln_speed_rpm",
    ];
    return keys.map((k) => {
      const b = Number((routineBefore as any)[k]);
      const a = Number((routineAfter as any)[k]);
      const d = Number.isFinite(b) && Number.isFinite(a) ? a - b : undefined;
      const p = Number.isFinite(b) && b !== 0 && Number.isFinite(a) ? ((a - b) / b) * 100 : undefined;
      return { k, before: b, after: a, delta: d, pct: p };
    });
  }, [routineBefore, routineAfter]);

  const chartBox = useMeasure();

  // Use live, ticking countdown for the header chip
  const nextLabel = useMemo(() => {
    if (countdown === null || countdown === undefined) return "-";
    const m = Math.floor(countdown / 60);
    const ss = countdown % 60;
    return `${m}:${String(ss).padStart(2, "0")} to next routine`;
  }, [countdown]);

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <header className="sticky top-0 z-10 backdrop-blur bg-white/70 border-b border-slate-200">
        <div className="mx-auto max-w-7xl px-4 py-3 flex items-center gap-3">
          <div className="text-xl font-semibold">Plant Agent – Dashboard</div>
          <div className="ml-auto flex items-center gap-2 text-sm">
            <Chip tone="slate">ver {ver || "-"}</Chip>
            <Chip tone={health === "200" ? "green" : "rose"}>health {health || "-"}</Chip>
            <Chip tone="indigo">
              {lastRuns?.last_cron_routine ? `last routine: ${new Date(lastRuns.last_cron_routine).toLocaleString()}` : "last routine: -"}
            </Chip>
            <Chip tone="amber">{nextLabel}</Chip>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl p-4 space-y-6">
        {/* Connection */}
        <section className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
          <div className="flex flex-col md:flex-row gap-3 md:items-end">
            <div className="flex-1">
              <label className="text-xs text-slate-500">API Base URL</label>
              <input value={baseRaw} onChange={(e) => setBaseRaw(e.target.value)} placeholder="https://<cloud-run-url>" className="w-full mt-1 px-3 py-2 border rounded-xl" />
              <div className="mt-1 text-[11px] text-slate-500">Using: <span className="font-mono">{base || "—"}</span></div>
            </div>
            <div className="flex-1">
              <label className="text-xs text-slate-500">ID Token (optional for private)</label>
              <input value={token} onChange={(e) => setToken(e.target.value)} placeholder="paste gcloud-issued ID token" className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <button onClick={fetchHealth} className="btn-outline" disabled={!base}>Check</button>
          </div>
          <div className="mt-3 flex items-center gap-3 text-sm">
            <label className="inline-flex items-center gap-2">
              <input type="checkbox" checked={autoPoll === "1"} onChange={(e) => setAutoPoll(e.target.checked ? "1" : "0")} /> Auto-refresh snapshot
            </label>
            <button onClick={() => { fetchSnapshot(); getMetrics(); fetchTrends(); fetchLastRuns(); }} className="btn-outline">Refresh now</button>
            {errorMsg ? <span className="text-rose-600">• {errorMsg}</span> : null}
          </div>
        </section>

        {/* KPI Tiles */}
        <section className="grid md:grid-cols-5 gap-4">
          {[
            { label: "Production (tph)", val: kpi?.production_tph },
            { label: "O₂ (%)", val: kpi?.o2_percent },
            { label: "Specific Power (kWh/t)", val: kpi?.specific_power_kwh_per_ton },
            { label: "Kiln Feed (tph)", val: kpi?.kiln_feed_tph },
            { label: "Separator ΔP (Pa)", val: kpi?.separator_dp_pa },
          ].map((t, idx) => (
            <div key={idx} className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
              <div className="text-xs text-slate-500">{t.label}</div>
              <div className="text-2xl font-semibold mt-1">{fmt(t.val, 3)}</div>
            </div>
          ))}
        </section>

        {/* Suggestions (Routine) */}
        <section className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
          <div className="flex items-center justify-between">
            <div className="font-semibold">Suggestions</div>
            <div className="text-xs text-slate-500">
              latest routine run: {routineOut?.created_at ? new Date(routineOut.created_at).toLocaleString() : "-"}
              {routineOut?.suggestion_id ? <span className="ml-2">• id: <span className="font-mono">{routineOut.suggestion_id}</span></span> : null}
            </div>
          </div>
          {(() => {
            const lines = deriveSuggestionLines(snap, routineOut?.proposed_setpoints, { o2_percent: { min: Number(o2Min), max: Number(o2Max) } });
            return lines.length ? (
              <ul className="mt-3 list-disc pl-6 text-sm space-y-1">{lines.map((s, i) => (<li key={i}>{s}</li>))}</ul>
            ) : (
              <div className="mt-3 text-sm text-slate-500">Run a routine optimization to populate suggestions.</div>
            );
          })()}
          <div className="mt-3 flex gap-2">
            <button onClick={applyRoutineProposal} disabled={!routineOut?.proposed_setpoints} className="btn-primary">Accept & Apply</button>
            <button onClick={rejectRoutine} className="btn-danger">Reject</button>
          </div>
        </section>

        {/* Trends + Snapshot table */}
        <section className="grid md:grid-cols-2 gap-4">
          <div className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
            <div className="font-semibold mb-1">Important Trends (last 2 hours)</div>
            <div className="text-xs text-slate-500 mb-2">{(trends.length ? trends : history).length ? `Loaded ${(trends.length ? trends : history).length} points` : "Waiting for data…"}</div>
            <div ref={chartBox.ref} className="h-56 w-full">
              {(trends.length ? trends : history).length > 0 && chartBox.w > 0 && chartBox.h > 0 ? (
                <LineChart width={chartBox.w} height={chartBox.h} data={trends.length ? trends : history} margin={{ top: 8, right: 32, left: 8, bottom: 8 }}>
                  <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
                  <XAxis dataKey="t" tick={{ fill: "#334155", fontSize: 12 }} axisLine={{ stroke: "#94a3b8" }} tickLine={{ stroke: "#94a3b8" }} />
                  <YAxis yAxisId="left" domain={["auto","auto"]} tick={{ fill: "#334155", fontSize: 12 }} axisLine={{ stroke: "#94a3b8" }} tickLine={{ stroke: "#94a3b8" }} />
                  <YAxis yAxisId="right" orientation="right" domain={["auto","auto"]} tick={{ fill: "#334155", fontSize: 12 }} axisLine={{ stroke: "#94a3b8" }} tickLine={{ stroke: "#94a3b8" }} />
                  <Tooltip /><Legend wrapperStyle={{ paddingTop: 8 }} />
                  <Line yAxisId="left" type="monotone" dataKey="production_tph" name="Production (tph)" stroke="#0ea5e9" strokeWidth={2} dot={{ r:2 }} isAnimationActive={false} connectNulls />
                  <Line yAxisId="right" type="monotone" dataKey="o2_percent" name="O₂ (%)" stroke="#22c55e" strokeWidth={2} dot={{ r:2 }} isAnimationActive={false} connectNulls />
                  <Line yAxisId="right" type="monotone" dataKey="specific_power_kwh_per_ton" name="Specific Power (kWh/t)" stroke="#f59e0b" strokeWidth={2} dot={{ r:2 }} isAnimationActive={false} connectNulls />
                </LineChart>
              ) : (
                <div className="h-full grid place-items-center text-slate-400 text-sm">No data to chart</div>
              )}
            </div>
          </div>

          <div className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
            <div className="font-semibold mb-2">Current Snapshot</div>
            {snap ? (
              <div className="grid grid-cols-2 gap-2 text-sm">
                {Object.entries(snap).map(([k, v]) => (
                  <div key={k} className="flex justify-between border-b py-1">
                    <span className="text-slate-500">{k}</span>
                    <span className="font-mono">{fmt(Number(v))}</span>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-slate-500 text-sm">No snapshot yet.</div>
            )}
          </div>
        </section>

        {/* Routine Controls */}
        <section className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
          <div className="flex items-center justify-between">
            <div className="font-semibold">Routine Optimization</div>
            <div className="text-xs text-slate-500">Logs to routine_suggestions_v2 (+ suggestions_v1)</div>
          </div>
          <div className="mt-3 grid md:grid-cols-6 gap-3 items-end">
            <div>
              <label className="text-xs text-slate-500">O₂ min</label>
              <input value={o2Min} onChange={(e) => setO2Min(e.target.value)} className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <div>
              <label className="text-xs text-slate-500">O₂ max</label>
              <input value={o2Max} onChange={(e) => setO2Max(e.target.value)} className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <label className="inline-flex items-center gap-2 text-sm">
              <input type="checkbox" checked={applyTop} onChange={(e) => setApplyTop(e.target.checked)} /> Apply top
            </label>
            <label className="inline-flex items-center gap-2 text-sm">
              <input type="checkbox" checked={logSugg} onChange={(e) => setLogSugg(e.target.checked)} /> Log suggestions
            </label>
            <button onClick={runRoutine} className="btn-secondary" disabled={!base}>Run routine</button>
            <div className="text-xs text-slate-500">Next: {lastRuns?.next_cron_eta ? new Date(lastRuns.next_cron_eta).toLocaleTimeString() : "-"}</div>
          </div>
        </section>

        {/* Load Planning */}
        <section className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
          <div className="flex items-center justify-between">
            <div className="font-semibold">Load Planning</div>
            <div className="text-xs text-slate-500">latest plan: {loadOut?.created_at ? new Date(loadOut.created_at).toLocaleString() : "-"} • id: {loadOut?.plan_id || "-"}</div>
          </div>
          <div className="mt-3 grid md:grid-cols-7 gap-3 items-end">
            <div>
              <label className="text-xs text-slate-500">Approach</label>
              <select value={loadMode} onChange={(e) => setLoadMode(e.target.value as any)} className="w-full mt-1 px-3 py-2 border rounded-xl">
                <option value="pct">delta_pct %</option>
                <option value="abs">delta_abs (tph)</option>
                <option value="target">target_tph</option>
              </select>
            </div>
            <div>
              <label className="text-xs text-slate-500">Value</label>
              <input value={val} onChange={(e) => setVal(e.target.value)} className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <div>
              <label className="text-xs text-slate-500">Steps</label>
              <input value={steps} onChange={(e) => setSteps(e.target.value)} className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <div>
              <label className="text-xs text-slate-500">Direction</label>
              <select value={direction} onChange={(e) => setDirection(e.target.value as any)} className="w-full mt-1 px-3 py-2 border rounded-xl">
                <option value="up">up</option>
                <option value="down">down</option>
              </select>
            </div>
            <div className="col-span-2">
              <label className="text-xs text-slate-500">Step dwell seconds (CSV; per-stage)</label>
              <input value={stepDwellCsv} onChange={(e) => setStepDwellCsv(e.target.value)} className="w-full mt-1 px-3 py-2 border rounded-xl font-mono" />
            </div>
            <button onClick={runLoad} className="btn-indigo" disabled={!base}>Create plan</button>
          </div>

          {loadOut && (
            <div className="mt-4">
              <div className="text-sm text-slate-600">mode: {loadOut.mode}</div>
              <div className="mt-2 grid md:grid-cols-2 gap-4">
                <div>
                  <div className="text-sm font-medium mb-1">Stages</div>
                  <div className="space-y-2">
                    {loadOut.stages.map((stg, i) => (
                      <div key={i} className="border rounded-xl p-3">
                        <div className="flex items-center justify-between">
                          <div className="font-medium">{stg.name || `Stage ${i + 1}`}</div>
                          <button onClick={() => applyStage(i)} className="btn-secondary">Apply</button>
                        </div>
                        <pre className="text-xs bg-slate-50 border rounded-xl p-3 mt-2 overflow-auto">{JSON.stringify(stg.setpoints, null, 2)}</pre>
                      </div>
                    ))}
                  </div>
                  <button onClick={acceptPlan} className="mt-3 btn-primary">Accept & Apply All</button>
                </div>
                <div>
                  <div className="text-sm font-medium mb-1">BigQuery log</div>
                  <pre className="text-xs bg-slate-50 border rounded-xl p-3 overflow-auto">{JSON.stringify(loadOut.bq_log, null, 2)}</pre>
                </div>
              </div>
            </div>
          )}
        </section>

        {/* Final After Routine Summary */}
        {(routineBefore && routineAfter && finalDeltaRoutine) && (
          <section className="bg-white rounded-2xl shadow-sm border border-emerald-200 p-4">
            <div className="flex items-center justify-between">
              <div className="font-semibold">Final Changes After Routine</div>
              <div className="text-xs text-slate-500">id: {routineOut?.suggestion_id || "-"}</div>
            </div>
            <div className="mt-3 overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="text-left text-slate-500 border-b">
                    <th className="py-2 pr-4">Metric</th><th className="py-2 pr-4">Before</th><th className="py-2 pr-4">After</th><th className="py-2 pr-4">Δ</th><th className="py-2 pr-4">Δ%</th>
                  </tr>
                </thead>
                <tbody>
                  {finalDeltaRoutine.map((r) => (
                    <tr key={r.k} className="border-b last:border-0">
                      <td className="py-2 pr-4 font-medium">{String(r.k)}</td>
                      <td className="py-2 pr-4 font-mono">{fmt(r.before)}</td>
                      <td className="py-2 pr-4 font-mono">{fmt(r.after)}</td>
                      <td className={cls("py-2 pr-4 font-mono", (r.delta ?? 0) >= 0 ? "text-emerald-700" : "text-rose-700")}>
                        {r.delta !== undefined ? (r.delta >= 0 ? "+" : "") + fmt(r.delta) : "-"}
                      </td>
                      <td className={cls("py-2 pr-4 font-mono", (r.pct ?? 0) >= 0 ? "text-emerald-700" : "text-rose-700")}>
                        {r.pct !== undefined ? (r.pct >= 0 ? "+" : "") + fmt(r.pct, 2) + "%" : "-"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {/* Final After Load-Up Summary */}
        {(loadBefore && loadAfter && finalDeltaLoad) && (
          <section className="bg-white rounded-2xl shadow-sm border border-emerald-200 p-4">
            <div className="flex items-center justify-between">
              <div className="font-semibold">Final Changes After Load-Up</div>
              <div className="text-xs text-slate-500">plan id: {loadOut?.plan_id || "-"}</div>
            </div>
            <div className="mt-3 overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="text-left text-slate-500 border-b">
                    <th className="py-2 pr-4">Metric</th><th className="py-2 pr-4">Before</th><th className="py-2 pr-4">After</th><th className="py-2 pr-4">Δ</th><th className="py-2 pr-4">Δ%</th>
                  </tr>
                </thead>
                <tbody>
                  {finalDeltaLoad.map((r) => (
                    <tr key={r.k} className="border-b last:border-0">
                      <td className="py-2 pr-4 font-medium">{String(r.k)}</td>
                      <td className="py-2 pr-4 font-mono">{fmt(r.before)}</td>
                      <td className="py-2 pr-4 font-mono">{fmt(r.after)}</td>
                      <td className={cls("py-2 pr-4 font-mono", (r.delta ?? 0) >= 0 ? "text-emerald-700" : "text-rose-700")}>
                        {r.delta !== undefined ? (r.delta >= 0 ? "+" : "") + fmt(r.delta) : "-"}
                      </td>
                      <td className={cls("py-2 pr-4 font-mono", (r.pct ?? 0) >= 0 ? "text-emerald-700" : "text-rose-700")}>
                        {r.pct !== undefined ? (r.pct >= 0 ? "+" : "") + fmt(r.pct, 2) + "%" : "-"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {/* Metrics */}
        <section className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
          <div className="flex items-center justify-between">
            <div className="font-semibold">Metrics</div>
            <button onClick={getMetrics} className="btn-outline">Refresh</button>
          </div>
          <pre className="text-xs bg-slate-50 border rounded-xl p-3 overflow-auto mt-2">{JSON.stringify(metrics, null, 2)}</pre>
        </section>
      </main>
    </div>
  );
}
