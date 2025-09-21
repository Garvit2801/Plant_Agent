import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend, CartesianGrid } from "recharts";

/**
 * Plant Agent – Operations Dashboard UI (BQ-aware)
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

const LS_KEYS = {
  BASE: "plant_ui.base_url",
  TOKEN: "plant_ui.id_token",
  AUTOPOLL: "plant_ui.autopoll",
};

function useLocalStorage(key: string, initial: string) {
  const [v, setV] = useState<string>(() => localStorage.getItem(key) ?? initial);
  useEffect(() => {
    localStorage.setItem(key, v);
  }, [key, v]);
  return [v, setV] as const;
}

function cls(...xs: (string | false | undefined | null)[]) {
  return xs.filter(Boolean).join(" ");
}

function fmt(n: number | undefined | null, d = 3) {
  if (n === undefined || n === null || Number.isNaN(n)) return "-";
  return Number(n).toFixed(d);
}

function safeNum(n: any): number | undefined {
  const x = Number(n);
  return Number.isFinite(x) ? x : undefined;
}

function formatSuggestionText(
  lever: string,
  current: number | undefined,
  proposed: number | undefined,
  deltaPct: number | undefined,
  cmin?: number,
  cmax?: number
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
  const b =
    cmin !== undefined || cmax !== undefined
      ? ` (bounds ${cmin !== undefined ? fmt(cmin) : "−∞"}…${cmax !== undefined ? fmt(cmax) : "+∞"})`
      : "";
  return base + b;
}

function deriveSuggestionLines(
  current: Snapshot | null,
  proposed: Record<string, number> | undefined,
  constraints?: Record<string, any>
) {
  if (!current || !proposed) return [] as string[];
  const out: string[] = [];
  for (const [lever, p] of Object.entries(proposed)) {
    const cur = safeNum((current as any)[lever]);
    const prop = safeNum(p);
    const deltaAbs = cur !== undefined && prop !== undefined ? prop - cur : undefined;
    const deltaPct = cur && cur !== 0 && prop !== undefined ? ((prop - cur) / cur) * 100 : undefined;
    const cMin = constraints?.[lever]?.min;
    const cMax = constraints?.[lever]?.max;
    const line =
      formatSuggestionText(lever, cur, prop, deltaPct, cMin, cMax) +
      (deltaAbs !== undefined ? ` (Δabs ${deltaAbs >= 0 ? "+" : ""}${Number(deltaAbs.toFixed(3))})` : "");
    out.push(line);
  }
  return out;
}

function Chip({ children, tone = "slate" }: { children: React.ReactNode; tone?: "slate" | "green" | "rose" | "amber" }) {
  const map: Record<string, string> = {
    slate: "bg-slate-100 text-slate-700",
    green: "bg-green-100 text-green-700",
    rose: "bg-rose-100 text-rose-700",
    amber: "bg-amber-100 text-amber-800",
  };
  return <span className={cls("px-2 py-1 rounded-full border text-xs", map[tone])}>{children}</span>;
}

/** Measure a container (fixes ResponsiveContainer zero-size issues) */
function useMeasure() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({ w: 0, h: 0 });
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const cr = e.contentRect;
        setSize({ w: Math.floor(cr.width), h: Math.floor(cr.height) });
      }
    });
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return { ref, ...size };
}

export default function PlantAgentDashboard() {
  const [base, setBase] = useLocalStorage(LS_KEYS.BASE, "");
  const [token, setToken] = useLocalStorage(LS_KEYS.TOKEN, "");
  const [autoPoll, setAutoPoll] = useLocalStorage(LS_KEYS.AUTOPOLL, "1");

  const headers = useMemo(() => {
    const h: Record<string, string> = { "Content-Type": "application/json" };
    if (token.trim()) h["Authorization"] = `Bearer ${token.trim()}`;
    return h;
  }, [token]);

  const [health, setHealth] = useState<string>("");
  const [ver, setVer] = useState<string>("");
  const [errorMsg, setErrorMsg] = useState<string>("");

  const fetchHealth = useCallback(async () => {
    setErrorMsg("");
    try {
      const r = await fetch(`${base}/health`, { headers });
      setHealth(`${r.status}`);
      const v = await fetch(`${base}/version`, { headers });
      const j = await v.json();
      setVer(j.version ?? "");
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
    const point: TrendPoint = {
      t: tLabel ?? new Date().toLocaleTimeString(),
      production_tph: s.production_tph,
      o2_percent: s.o2_percent,
      specific_power_kwh_per_ton: s.specific_power_kwh_per_ton,
    };
    setHistory((prev) => [...prev.slice(-120), point]);
  }, []);

  /** Prefer non-BQ snapshot first (avoids view lag); fallback to BQ */
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
      } catch {
        /* try next */
      }
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
      const r = await fetch(`${base}/trends?minutes=60&limit=120`, { headers });
      if (!r.ok) {
        if (r.status === 404) return;
        throw new Error(`/trends ${r.status}`);
      }
      const data = await r.json();
      if (Array.isArray(data)) {
        const mapped: TrendPoint[] = data
          .map((row: any) => ({
            t: row.ts ? new Date(row.ts).toLocaleTimeString() : "",
            production_tph: Number(row.production_tph),
            o2_percent: Number(row.o2_percent),
            specific_power_kwh_per_ton: Number(row.specific_power_kwh_per_ton),
          }))
          .filter(
            (p) =>
              Number.isFinite(p.production_tph) &&
              Number.isFinite(p.o2_percent) &&
              Number.isFinite(p.specific_power_kwh_per_ton)
          );
        setTrends(mapped.slice(-120));
      }
    } catch (e: any) {
      setErrorMsg((prev) => prev || e?.message || "Failed to fetch trends");
    }
  }, [base, headers]);

  useEffect(() => {
    if (!base) return;
    fetchHealth();
    fetchTrends();
    fetchSnapshot();
  }, [base]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    const enabled = autoPoll === "1";
    if (!enabled || !base) {
      if (pollRef.current) window.clearInterval(pollRef.current);
      pollRef.current = null;
      return;
    }
    pollRef.current = window.setInterval(() => {
      fetchSnapshot().catch(() => {});
    }, 5000);
    return () => {
      if (pollRef.current) window.clearInterval(pollRef.current);
      pollRef.current = null;
    };
  }, [autoPoll, base, fetchSnapshot]);

  // Metrics
  const [metrics, setMetrics] = useState<any>(null);
  const getMetrics = useCallback(async () => {
    setErrorMsg("");
    try {
      const r = await fetch(`${base}/metrics`, { headers });
      if (!r.ok) throw new Error(`/metrics ${r.status}`);
      setMetrics(await r.json());
    } catch (e: any) {
      setErrorMsg(e?.message || "Failed to fetch metrics");
    }
  }, [base, headers]);

  // Routine
  const [o2Min, setO2Min] = useState<string>("2.3");
  const [o2Max, setO2Max] = useState<string>("4.5");
  const [applyTop, setApplyTop] = useState<boolean>(false);
  const [logSugg, setLogSugg] = useState<boolean>(true);
  const [routineOut, setRoutineOut] = useState<RoutineResp | null>(null);

  // Routine before/after (for accepted suggestions)
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
      // capture BEFORE
      const s0 = (await fetchSnapshotFast()) ?? snap;
      if (s0) setRoutineBefore({ ...s0 });

      const r = await fetch(`${base}/optimize/routine`, { method: "POST", headers, body: JSON.stringify(body) });
      const j: RoutineResp = await r.json();
      setRoutineOut(j);

      // if backend auto-applied (apply_top), prefer its actuation.after
      if (j.actuation?.after) {
        const after = j.actuation.after as Snapshot;
        setSnap(after);
        pushHistory(after);
        setRoutineAfter({ ...after });
      }
    } catch (e: any) {
      setErrorMsg(e?.message || "Run routine failed");
    }
  }, [base, headers, o2Min, o2Max, applyTop, logSugg, fetchSnapshotFast, snap, pushHistory]);

  const applyRoutineProposal = useCallback(async () => {
    if (!routineOut?.proposed_setpoints) return;
    setErrorMsg("");
    try {
      const body = { proposal: routineOut.proposed_setpoints, mode: "routine" };
      const r = await fetch(`${base}/actuate/apply_stage`, { method: "POST", headers, body: JSON.stringify(body) });
      const j: ApplyResp = await r.json();

      if (j.after) {
        const after = j.after as Snapshot;
        setSnap(after);
        pushHistory(after);
        setRoutineAfter({ ...after });
      } else {
        const s1 = await fetchSnapshotFast();
        if (s1) {
          setSnap(s1);
          setRoutineAfter({ ...s1 });
        }
      }
    } catch (e: any) {
      setErrorMsg(e?.message || "Apply failed");
    }
  }, [base, headers, routineOut, fetchSnapshotFast, pushHistory]);

  const rejectRoutine = useCallback(() => {
    setRoutineOut((x) => (x ? { ...x, applied: false } : x));
    setRoutineBefore(null);
    setRoutineAfter(null);
  }, []);

  const suggestionLines = useMemo(
    () =>
      deriveSuggestionLines(snap, routineOut?.proposed_setpoints, {
        o2_percent: { min: Number(o2Min), max: Number(o2Max) },
      }),
    [snap, routineOut, o2Min, o2Max]
  );

  // Load planning
  const [loadMode, setLoadMode] = useState<"pct" | "abs" | "target">("pct");
  const [steps, setSteps] = useState<string>("3");
  const [direction, setDirection] = useState<"auto" | "up" | "down">("auto");
  const [val, setVal] = useState<string>("8");
  const [loadOut, setLoadOut] = useState<LoadResp | null>(null);

  // Load-up before/after
  const [loadBefore, setLoadBefore] = useState<Snapshot | null>(null);
  const [loadAfter, setLoadAfter] = useState<Snapshot | null>(null);

  const runLoad = useCallback(async () => {
    setErrorMsg("");
    try {
      const body: LoadReq = { steps: Number(steps) || 3 } as any;
      if (direction !== "auto") body.direction = direction;
      if (loadMode === "pct") body.delta_pct = Number(val);
      if (loadMode === "abs") body.delta_abs = Number(val);
      if (loadMode === "target") body.target_tph = Number(val);

      const r = await fetch(`${base}/optimize/load`, { method: "POST", headers, body: JSON.stringify(body) });
      const j: LoadResp = await r.json();
      setLoadOut(j);

      const s0 = (await fetchSnapshotFast()) ?? snap;
      if (s0) setLoadBefore({ ...s0 });
      setLoadAfter(null);
    } catch (e: any) {
      setErrorMsg(e?.message || "Create plan failed");
    }
  }, [base, headers, steps, direction, val, loadMode, fetchSnapshotFast, snap]);

  const applyStage = useCallback(
    async (i: number) => {
      if (!loadOut) return;
      setErrorMsg("");
      try {
        const body = { stage: loadOut.stages[i], mode: loadOut.mode, plan_id: loadOut.plan_id, stage_index: i };
        const r = await fetch(`${base}/actuate/apply_stage`, { method: "POST", headers, body: JSON.stringify(body) });
        const j: ApplyResp = await r.json();

        if (j.after) {
          const after = j.after as Snapshot;
          setSnap(after);
          pushHistory(after);
          if (i === loadOut.stages.length - 1) setLoadAfter({ ...after });
        } else if (i === loadOut.stages.length - 1) {
          const s1 = await fetchSnapshotFast();
          if (s1) setLoadAfter({ ...s1 });
        }
      } catch (e: any) {
        setErrorMsg(e?.message || `Apply stage ${i + 1} failed`);
      }
    },
    [base, headers, loadOut, fetchSnapshotFast, pushHistory]
  );

  const applyAllStages = useCallback(async () => {
    if (!loadOut) return;
    if (!loadBefore) {
      const s0 = (await fetchSnapshotFast()) ?? snap;
      if (s0) setLoadBefore({ ...s0 });
    }
    for (let i = 0; i < loadOut.stages.length; i++) {
      await applyStage(i);
      await new Promise((res) => setTimeout(res, 400));
    }
    if (!loadAfter) {
      const s1 = await fetchSnapshotFast();
      if (s1) setLoadAfter({ ...s1 });
    }
  }, [loadOut, applyStage, loadBefore, fetchSnapshotFast, snap, loadAfter]);

  const acceptPlan = useCallback(async () => {
    if (!loadOut) return;
    await applyAllStages();
  }, [loadOut, applyAllStages]);

  const rejectPlan = useCallback(() => {
    setLoadOut(null);
    setLoadBefore(null);
    setLoadAfter(null);
  }, []);

  const disabled = !base;
  const kpi = snap;
  const chartData: TrendPoint[] = trends.length ? trends : history;

  const finalDeltaLoad = useMemo(() => {
    if (!loadBefore || !loadAfter) return null;
    const keys: (keyof Snapshot)[] = [
      "production_tph",
      "o2_percent",
      "specific_power_kwh_per_ton",
      "kiln_feed_tph",
      "separator_dp_pa",
      "id_fan_flow_Nm3_h",
      "cooler_airflow_Nm3_h",
      "kiln_speed_rpm",
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
      "production_tph",
      "o2_percent",
      "specific_power_kwh_per_ton",
      "kiln_feed_tph",
      "separator_dp_pa",
      "id_fan_flow_Nm3_h",
      "cooler_airflow_Nm3_h",
      "kiln_speed_rpm",
    ];
    return keys.map((k) => {
      const b = Number((routineBefore as any)[k]);
      const a = Number((routineAfter as any)[k]);
      const d = Number.isFinite(b) && Number.isFinite(a) ? a - b : undefined;
      const p = Number.isFinite(b) && b !== 0 && Number.isFinite(a) ? ((a - b) / b) * 100 : undefined;
      return { k, before: b, after: a, delta: d, pct: p };
    });
  }, [routineBefore, routineAfter]);

  // measured chart box
  const chartBox = useMeasure();

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <header className="sticky top-0 z-10 backdrop-blur bg-white/70 border-b border-slate-200">
        <div className="mx-auto max-w-7xl px-4 py-3 flex items-center gap-3">
          <div className="text-xl font-semibold">Plant Agent – Dashboard</div>
          <div className="ml-auto flex items-center gap-2 text-sm">
            <Chip tone="slate">ver {ver || "-"}</Chip>
            <Chip tone={health === "200" ? "green" : "rose"}>health {health || "-"}</Chip>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl p-4 space-y-6">
        {/* Connection */}
        <section className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
          <div className="flex flex-col md:flex-row gap-3 md:items-end">
            <div className="flex-1">
              <label className="text-xs text-slate-500">API Base URL</label>
              <input value={base} onChange={(e) => setBase(e.target.value)} placeholder="https://<cloud-run-url>" className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <div className="flex-1">
              <label className="text-xs text-slate-500">ID Token (optional for private)</label>
              <input value={token} onChange={(e) => setToken(e.target.value)} placeholder="paste gcloud-issued ID token" className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <button onClick={fetchHealth} className="btn-secondary" disabled={disabled}>Check</button>
          </div>
          <div className="mt-3 flex items-center gap-3 text-sm">
            <label className="inline-flex items-center gap-2"><input type="checkbox" checked={autoPoll === "1"} onChange={(e) => setAutoPoll(e.target.checked ? "1" : "0")} /> Auto-refresh snapshot</label>
            <button onClick={() => { fetchSnapshot(); getMetrics(); fetchTrends(); }} className="btn-outline">Refresh now</button>
            {errorMsg ? <span className="text-rose-600">• {errorMsg}</span> : null}
          </div>
        </section>

        {/* KPI Tiles */}
        <section className="grid md:grid-cols-5 gap-4">
          {[{
            label: "Production (tph)", val: kpi?.production_tph
          }, {
            label: "O₂ (%)", val: kpi?.o2_percent
          }, {
            label: "Specific Power (kWh/t)", val: kpi?.specific_power_kwh_per_ton
          }, {
            label: "Kiln Feed (tph)", val: kpi?.kiln_feed_tph
          }, {
            label: "Separator ΔP (Pa)", val: kpi?.separator_dp_pa
          }].map((t, idx) => (
            <div key={idx} className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
              <div className="text-xs text-slate-500">{t.label}</div>
              <div className="text-2xl font-semibold mt-1">{fmt(t.val, 3)}</div>
            </div>
          ))}
        </section>

        {/* Suggestions */}
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
              <ul className="mt-3 list-disc pl-6 text-sm space-y-1">
                {lines.map((s, i) => (<li key={i}>{s}</li>))}
              </ul>
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
            <div className="font-semibold mb-1">Important Trends (last hour)</div>
            <div className="text-xs text-slate-500 mb-2">{chartData.length ? `Loaded ${chartData.length} points` : "Waiting for data…"}</div>

            {/* Measured box fixes invisible chart */}
            <div ref={chartBox.ref} className="h-56 w-full">
              {chartData.length > 0 && chartBox.w > 0 && chartBox.h > 0 ? (
                <LineChart width={chartBox.w} height={chartBox.h} data={chartData} margin={{ top: 8, right: 32, left: 8, bottom: 8 }}>
                  <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
                  <XAxis dataKey="t" tick={{ fill: "#334155", fontSize: 12 }} axisLine={{ stroke: "#94a3b8" }} tickLine={{ stroke: "#94a3b8" }} />
                  {/* Left axis: production (tph) */}
                  <YAxis
                    yAxisId="left"
                    domain={["auto", "auto"]}
                    tick={{ fill: "#334155", fontSize: 12 }}
                    axisLine={{ stroke: "#94a3b8" }}
                    tickLine={{ stroke: "#94a3b8" }}
                  />
                  {/* Right axis: O2% + Specific Power */}
                  <YAxis
                    yAxisId="right"
                    orientation="right"
                    domain={["auto", "auto"]}
                    tick={{ fill: "#334155", fontSize: 12 }}
                    axisLine={{ stroke: "#94a3b8" }}
                    tickLine={{ stroke: "#94a3b8" }}
                  />
                  <Tooltip />
                  <Legend wrapperStyle={{ paddingTop: 8 }} />
                  <Line yAxisId="left" type="monotone" dataKey="production_tph" name="Production (tph)" stroke="#0ea5e9" strokeWidth={2} dot={{ r: 2 }} isAnimationActive={false} connectNulls />
                  <Line yAxisId="right" type="monotone" dataKey="o2_percent" name="O₂ (%)" stroke="#22c55e" strokeWidth={2} dot={{ r: 2 }} isAnimationActive={false} connectNulls />
                  <Line yAxisId="right" type="monotone" dataKey="specific_power_kwh_per_ton" name="Specific Power (kWh/t)" stroke="#f59e0b" strokeWidth={2} dot={{ r: 2 }} isAnimationActive={false} connectNulls />
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
          <div className="mt-3 grid md:grid-cols-5 gap-3 items-end">
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
            <button onClick={runRoutine} className="btn-secondary" disabled={disabled}>Run routine</button>
          </div>
        </section>

        {/* Load Planning */}
        <section className="bg-white rounded-2xl shadow-sm border border-slate-200 p-4">
          <div className="flex items-center justify-between">
            <div className="font-semibold">Load Planning</div>
            <div className="text-xs text-slate-500">
              latest plan: {loadOut?.created_at ? new Date(loadOut.created_at).toLocaleString() : "-"} • id: {loadOut?.plan_id || "-"}
            </div>
          </div>
          <div className="mt-3 grid md:grid-cols-6 gap-3 items-end">
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
                <option value="auto">auto</option>
                <option value="up">up</option>
                <option value="down">down</option>
              </select>
            </div>
            <button onClick={runLoad} className="btn-indigo" disabled={disabled}>Create plan</button>
            {loadOut && (
              <div className="flex gap-2">
                <button onClick={acceptPlan} className="btn-primary">Accept & Apply All</button>
                <button onClick={rejectPlan} className="btn-danger">Reject</button>
              </div>
            )}
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
                  <button onClick={applyAllStages} className="mt-3 btn-outline">Apply all</button>
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
                    <th className="py-2 pr-4">Metric</th>
                    <th className="py-2 pr-4">Before</th>
                    <th className="py-2 pr-4">After</th>
                    <th className="py-2 pr-4">Δ</th>
                    <th className="py-2 pr-4">Δ%</th>
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
                    <th className="py-2 pr-4">Metric</th>
                    <th className="py-2 pr-4">Before</th>
                    <th className="py-2 pr-4">After</th>
                    <th className="py-2 pr-4">Δ</th>
                    <th className="py-2 pr-4">Δ%</th>
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
