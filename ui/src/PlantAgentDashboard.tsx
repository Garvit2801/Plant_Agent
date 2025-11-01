import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend, CartesianGrid } from "recharts";

/**
 * Plant Agent – Operations Dashboard (Updated)
 * - Auto-apply toggle for routine (defaults OFF)
 * - Shows flags_effective (apply_top/log_suggestions/nudge_if_neutral)
 * - Per-lever audit table (decision / rule / deltas / bounds)
 * - Diagnostics footer (/metrics + /debug/last_runs)
 * - Kept your trends, load planning, and apply flows
 */

const RECOMMENDED_HOST = "https://plant-agent-i32khy5nrq-el.a.run.app";
const DEBUG_KEY = "plant_ui.debug";
const getDebug = () => (localStorage.getItem(DEBUG_KEY) ?? "0") === "1";
const setDebug = (b: boolean) => localStorage.setItem(DEBUG_KEY, b ? "1" : "0");

const LS_KEYS = {
  BASE: "plant_ui.base_url",
  TOKEN: "plant_ui.id_token",
  AUTOPOLL: "plant_ui.autopoll",
  STEP_DWELL: "plant_ui.step_dwell",
  TREND_SOURCE: "plant_ui.trend_source",
  NUDGE: "plant_ui.nudge_if_neutral",
  AUTO_APPLY: "plant_ui.routine_auto_apply",
};

function useLocalStorage(key: string, initial: string) {
  const [v, setV] = useState<string>(() => localStorage.getItem(key) ?? initial);
  useEffect(() => { localStorage.setItem(key, v); }, [key, v]);
  return [v, setV] as const;
}

function cls(...xs: (string | false | undefined | null)[]) { return xs.filter(Boolean).join(" "); }
function fmt(n: number | undefined | null, d = 3) { if (n == null || Number.isNaN(n)) return "-"; return Number(n).toFixed(d); }
function normalizeBase(u: string) { if (!u) return ""; let s = u.trim(); s = s.replace(/\/+$/, ""); s = s.replace(/\/snapshot$/i, ""); return s; }
function info(msg: string, extra?: any) { if (getDebug()) console.log(`[PlantUI] ${msg}`, extra ?? ""); }
function warn(msg: string, extra?: any) { console.warn(`[PlantUI] ${msg}`, extra ?? ""); }

/* =========================
   Types (align to backend)
   ========================= */
type Snapshot = {
  production_tph: number; kiln_feed_tph: number; separator_dp_pa: number;
  id_fan_flow_Nm3_h: number; cooler_airflow_Nm3_h: number; kiln_speed_rpm: number;
  o2_percent: number; specific_power_kwh_per_ton: number;
};

type TrendPoint = { t: string; production_tph: number | null; o2_percent: number | null; specific_power_kwh_per_ton: number | null; };

type LastRuns = { last_cron_routine: string | null; last_ingest: string | null; sched_period_sec: number; now: string; next_cron_eta: string | null; seconds_to_next: number | null; };

type LeverAudit = {
  current: number | null;
  proposed_before_filter: number | null;
  proposed?: number;
  delta_abs: number | null;
  delta_pct: number | null; // fraction (e.g., 0.012 for +1.2%)
  bounds: { min?: number; max?: number };
  decision: "keep" | "skip_small" | "skip_missing" | "skip_missing_neutral" | "nudge" | "pending";
  rule: string | null;
};

type ApplyResp = { ok: boolean; before?: Partial<Snapshot> | null; after?: Partial<Snapshot> | null; applied_at?: string; bq_log?: { table?: string | null; insert_error?: string | null }; note?: string; error?: string };

type RoutineResp = {
  mode: "routine";
  current: Snapshot;
  predicted_after?: { specific_power_kwh_per_ton?: number };
  actions?: { apply_stage?: boolean; apply_all?: boolean; rollback?: boolean };
  proposed_setpoints?: Record<string, number>;
  per_lever?: Record<string, LeverAudit>;
  reason?: "proposed" | "neutral" | "nudge_applied" | string;
  reason_detail?: string;
  match_info?: any;
  used_snapshot_source?: string;
  used_snapshot_ts?: string;
  used_snapshot_hash?: string;
  flags_effective?: { apply_top: boolean; log_suggestions: boolean; nudge_if_neutral: boolean };
  constraints?: any;
  targets?: any;
  bq_log?: { table?: string | null; insert_error?: string | null };
  suggestions_log?: { table?: string | null; insert_error?: string | null; rows_inserted?: number };
  applied?: boolean;
  actuation?: ApplyResp | null;
  suggestion_id?: string;
  created_at?: string;
};

type LoadResp = {
  plan_id: string; created_at: string; mode: "load_up" | "load_down";
  current: Snapshot;
  predicted_after?: { specific_power_kwh_per_ton?: number };
  actions?: { apply_stage?: boolean; apply_all?: boolean; rollback?: boolean };
  stages: { name?: string; checks?: string[]; setpoints: Record<string, number> }[];
  target: { production_tph: number; delta_pct?: number | null; delta_abs?: number | null; requested?: any };
  bq_log?: { table?: string | null; insert_error?: string | null };
  match_info?: any; targets?: any; steps_cfg?: any;
};

/* =========================
   Helpers used by UI
   ========================= */
function pickLatestTimestamp(j: any): string | null { return j?.created_at || j?.createdAt || j?.ts || j?.timestamp || null; }
function pickProposal(j: any): Record<string, number> | undefined { return j?.proposed_setpoints || j?.proposal?.setpoints || j?.top?.proposed_setpoints; }

async function getJsonCandidates(base: string, headers: Record<string, string>, paths: string[]) {
  let last: any;
  for (const p of paths) {
    const url = `${base}${p}`;
    try {
      const r = await fetch(url, { headers });
      if (r.ok) return { json: await r.json(), path: p, status: r.status, method: "GET" as const };
      const txt = await r.text().catch(()=> "");
      last = new Error(`GET ${p} ${r.status}${txt ? ` – ${txt.slice(0,140)}` : ""}`);
    } catch (e) { last = e; }
  }
  throw last ?? new Error("No candidate path succeeded");
}

async function methodFallback(base: string, getHeaders: Record<string,string>, postHeaders: Record<string,string>, paths: string[]) {
  let last: any;
  for (const p of paths) {
    const url = `${base}${p}`;
    try {
      const g = await fetch(url, { headers: getHeaders });
      if (g.ok) return { json: await g.json(), path: p, method: "GET" as const, status: g.status };
      if (g.status === 405) {
        try {
          const pr = await fetch(url, { method: "POST", headers: postHeaders, body: "{}" });
          if (pr.ok) return { json: await pr.json(), path: p, method: "POST" as const, status: pr.status };
          const txt = await pr.text().catch(()=> "");
          last = new Error(`POST ${p} ${pr.status}${txt ? ` – ${txt.slice(0,140)}` : ""}`);
        } catch (e) { last = e; }
      } else {
        const txt = await g.text().catch(()=> "");
        last = new Error(`GET ${p} ${g.status}${txt ? ` – ${txt.slice(0,140)}` : ""}`);
      }
    } catch (e) { last = e; }
  }
  throw last ?? new Error("No candidate path via GET/POST");
}

/* trend cleaning (unchanged) */
function cleanTrends(rows: any[]): TrendPoint[] {
  const sorted = rows.slice().sort((a,b)=>new Date(a.ts??0).getTime()-new Date(b.ts??0).getTime());
  const out: TrendPoint[] = [];
  let lastProd: number | undefined;
  for (const row of sorted) {
    const parse = (v:any) => { if (v==null) return NaN; const s = typeof v === "string" ? v.replace(/,/g,"") : String(v); const n = Number(s); return Number.isFinite(n)?n:NaN; };
    const prod = parse(row.production_tph), o2 = parse(row.o2_percent), sp = parse(row.specific_power_kwh_per_ton);
    const prodGood = Number.isFinite(prod) && prod >= 0.5 && prod < 1000;
    const o2Good = Number.isFinite(o2) && o2 >= 0 && o2 < 30;
    const spGood = Number.isFinite(sp) && sp > 0 && sp < 200;
    const spike = lastProd && prodGood ? Math.abs(prod-lastProd)/Math.max(1e-9,lastProd) > 0.30 : false;
    const keepProd = prodGood && !spike ? prod : null;
    if (keepProd !== null) lastProd = keepProd;
    out.push({ t: row.ts ? new Date(row.ts).toLocaleTimeString() : "", production_tph: keepProd, o2_percent: o2Good?o2:null, specific_power_kwh_per_ton: spGood?sp:null });
  }
  return out;
}

/* =========================
   Main Component
   ========================= */
export default function PlantAgentDashboard() {
  const [baseRaw, setBaseRaw] = useLocalStorage(LS_KEYS.BASE, RECOMMENDED_HOST);
  const base = useMemo(() => normalizeBase(baseRaw), [baseRaw]);
  const [token, setToken] = useLocalStorage(LS_KEYS.TOKEN, "");
  const [autoPoll, setAutoPoll] = useLocalStorage(LS_KEYS.AUTOPOLL, "1");
  const [trendSource, setTrendSource] = useLocalStorage(LS_KEYS.TREND_SOURCE, "auto");
  const [stepDwellCsv, setStepDwellCsv] = useLocalStorage(LS_KEYS.STEP_DWELL, "20");
  const [nudgeFlag, setNudgeFlag] = useLocalStorage(LS_KEYS.NUDGE, "1");
  const [autoApplyRoutine, setAutoApplyRoutine] = useLocalStorage(LS_KEYS.AUTO_APPLY, "0"); // default OFF

  const getHeaders = useMemo(() => {
    const h: Record<string,string> = {};
    if (token.trim()) h.Authorization = `Bearer ${token.trim()}`;
    return h;
  }, [token]);
  const postHeaders = useMemo(() => {
    const h: Record<string,string> = {"Content-Type":"application/json"};
    if (token.trim()) h.Authorization = `Bearer ${token.trim()}`;
    return h;
  }, [token]);

  const [health, setHealth] = useState<string>("-");
  const [ver, setVer] = useState<string>("-");
  const [errorMsg, setErrorMsg] = useState<string>("");

  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [history, setHistory] = useState<TrendPoint[]>([]);
  const [trends, setTrends] = useState<TrendPoint[]>([]);
  const [trendEndpoint, setTrendEndpoint] = useState("(none)");

  const [lastRuns, setLastRuns] = useState<LastRuns | null>(null);
  const [countdown, setCountdown] = useState<number | null>(null);
  const [countdownCause, setCountdownCause] = useState<string>("");

  const [routineOut, setRoutineOut] = useState<RoutineResp | null>(null);
  const [routineRaw, setRoutineRaw] = useState<any>(null);
  const [latestSeenAt, setLatestSeenAt] = useState<string | null>(null);

  const [loadOut, setLoadOut] = useState<LoadResp | null>(null);
  const [loadBefore, setLoadBefore] = useState<Snapshot | null>(null);
  const [loadAfter, setLoadAfter] = useState<Snapshot | null>(null);

  const [o2Min, setO2Min] = useState("2.3");
  const [o2Max, setO2Max] = useState("4.5");
  const [applyTop, setApplyTop] = useState(false);
  const [logSugg, setLogSugg] = useState(true);
  const [routineBefore, setRoutineBefore] = useState<Snapshot | null>(null);
  const [routineAfter, setRoutineAfter] = useState<Snapshot | null>(null);

  const [metrics, setMetrics] = useState<any>(null); // diagnostics
  const [metricsErr, setMetricsErr] = useState<string>("");

  const pollRef = useRef<number | null>(null);
  const countTimerRef = useRef<number | null>(null);
  const disableLastRunsRef = useRef(false);
  const cronProbeRef = useRef<number | null>(null);
  const lastCronRef = useRef<string | null>(null);

  /* Health/version */
  const fetchHealth = useCallback(async () => {
    if (!base) return;
    try {
      const candidates = ["/health", "/snapshot/health", "/healthz"];
      let r: Response | null = null;
      for (const p of candidates) {
        try { const t = await fetch(`${base}${p}`, { headers: getHeaders }); r = t; if (t.ok || t.status !== 404) break; } catch {}
      }
      if (!r) throw new Error("health unreachable");
      setHealth(String(r.status));
      const { json: vj } = await getJsonCandidates(base, getHeaders, ["/version","/snapshot/version"]);
      setVer(vj?.version ?? "-");
    } catch (e:any) {
      setHealth("error"); setVer("-"); setErrorMsg(e?.message || "health failed");
    }
  }, [base, getHeaders]);

  /* Snapshot & trend helpers */
  const pushHistory = useCallback((s: Snapshot, tLabel?: string) => {
    setHistory(prev => [...prev.slice(-240), {
      t: tLabel ?? new Date().toLocaleTimeString(),
      production_tph: s.production_tph, o2_percent: s.o2_percent, specific_power_kwh_per_ton: s.specific_power_kwh_per_ton
    }]);
  }, []);
  const fetchSnapshotFast = useCallback(async (): Promise<Snapshot | null> => {
    if (!base) return null;
    for (const p of ["/snapshot","/snapshot?source=bq"]) {
      try {
        const r = await fetch(`${base}${p}`, { headers: getHeaders });
        if (!r.ok) continue;
        const j = await r.json(); setSnap(j); pushHistory(j); return j;
      } catch {}
    }
    return null;
  }, [base, getHeaders, pushHistory]);
  const fetchSnapshot = useCallback(async () => {
    const s = await fetchSnapshotFast();
    if (!s) setErrorMsg("Failed to fetch snapshot");
  }, [fetchSnapshotFast]);

  const fetchTrends = useCallback(async () => {
    if (!base) return;
    try {
      const pathAuto = "/trends?minutes=120&limit=240&source=auto";
      const pathBQ   = "/trends?minutes=120&limit=240&source=bq";
      const { json, path } = await getJsonCandidates(base, getHeaders, trendSource === "bq" ? [pathBQ,pathAuto] : [pathAuto,pathBQ]);
      setTrendEndpoint(path);
      if (Array.isArray(json)) setTrends(cleanTrends(json).slice(-240));
    } catch (e:any) {
      setErrorMsg(prev => prev || e?.message || "Failed to fetch trends");
    }
  }, [base, getHeaders, trendSource]);

  /* last_runs + countdown */
  const fetchLastRuns = useCallback(async () => {
    if (!base || disableLastRunsRef.current) return;
    try {
      const { json } = await methodFallback(base, getHeaders, postHeaders, [
        "/debug/last_runs", "/debug/last_runs/", "/snapshot/last_runs", "/snapshot/last_runs/",
        "/last_runs", "/last_runs/", "/debug/schedule", "/debug/schedule/"
      ]);
      const j = json as LastRuns;
      setLastRuns(j); setCountdown(j?.seconds_to_next ?? null); setCountdownCause("");
    } catch (e:any) {
      const msg = String(e?.message || e);
      if (/405|Method Not Allowed/i.test(msg)) disableLastRunsRef.current = true;
      setCountdown(null); setLastRuns(null); setCountdownCause(`Countdown unavailable: ${msg}`);
    }
  }, [base, getHeaders, postHeaders]);

  const fetchRoutineLatest = useCallback(async () => {
    if (!base) return false;
    try {
      const { json, path } = await getJsonCandidates(base, getHeaders, ["/routine/latest", "/routine/latest/"]);
      setRoutineRaw(json);
      const created = pickLatestTimestamp(json);
      const proposed = pickProposal(json);
      if (created) {
        const isNewer = !latestSeenAt || new Date(created).getTime() > new Date(latestSeenAt).getTime();
        if (isNewer) setLatestSeenAt(created);
      }
      if (proposed && Object.keys(proposed).length > 0) {
        setRoutineOut(json);
        info(`Fetched routine latest via ${path}`, json);
        return true;
      }
      return false;
    } catch {
      return false;
    }
  }, [base, getHeaders, latestSeenAt]);

  const startCronProbe = useCallback(() => {
    if (cronProbeRef.current) window.clearInterval(cronProbeRef.current);
    let elapsed = 0;
    cronProbeRef.current = window.setInterval(async () => {
      const ok = await fetchRoutineLatest();
      if (ok || elapsed >= 60000) {
        if (cronProbeRef.current) window.clearInterval(cronProbeRef.current);
        cronProbeRef.current = null;
      }
      elapsed += 2000;
    }, 2000);
  }, [fetchRoutineLatest]);

  useEffect(() => { if (countdown != null && countdown <= 5) startCronProbe(); }, [countdown, startCronProbe]);
  useEffect(() => {
    const ts = lastRuns?.last_cron_routine || null;
    if (!ts) return;
    if (!lastCronRef.current || new Date(ts).getTime() > new Date(lastCronRef.current).getTime()) {
      lastCronRef.current = ts; startCronProbe();
    }
  }, [lastRuns?.last_cron_routine, startCronProbe]);

  /* polling */
  useEffect(() => {
    const enabled = autoPoll === "1" && !!base;
    if (!enabled) { if (pollRef.current) window.clearInterval(pollRef.current); pollRef.current = null; return; }
    disableLastRunsRef.current = false;

    fetchSnapshot().catch(()=>{});
    fetchTrends().catch(()=>{});
    fetchLastRuns().catch(()=>{});
    fetchRoutineLatest().catch(()=>{});

    pollRef.current = window.setInterval(() => {
      fetchSnapshot().catch(()=>{});
      fetchLastRuns().catch(()=>{});
    }, 5000);
    return () => { if (pollRef.current) window.clearInterval(pollRef.current); pollRef.current = null; };
  }, [autoPoll, base, fetchSnapshot, fetchTrends, fetchLastRuns, fetchRoutineLatest]);

  /* countdown tick */
  useEffect(() => {
    if (countdown === null) { if (countTimerRef.current) window.clearInterval(countTimerRef.current); countTimerRef.current = null; return; }
    if (countTimerRef.current) window.clearInterval(countTimerRef.current);
    countTimerRef.current = window.setInterval(() => { setCountdown(c => (c==null ? c : Math.max(0, c - 1))); }, 1000);
    return () => { if (countTimerRef.current) window.clearInterval(countTimerRef.current); countTimerRef.current = null; };
  }, [countdown]);

  /* metrics (diagnostics) */
  const getMetrics = useCallback(async () => {
    if (!base) return;
    setMetricsErr("");
    try {
      const r = await fetch(`${base}/metrics`, { headers: getHeaders });
      if (!r.ok) throw new Error(`${r.status}`);
      const j = await r.json(); setMetrics(j);
    } catch (e:any) { setMetricsErr(e?.message || "Failed to fetch metrics"); }
  }, [base, getHeaders]);

  /* Routine actions */
  const runRoutine = useCallback(async () => {
    if (!base) return;
    setErrorMsg("");
    try {
      const body = {
        constraints: { o2_percent: { min: Number(o2Min)||undefined, max: Number(o2Max)||undefined } },
        // UI toggles (explicit): default auto-apply OFF unless user enables it
        apply_top: autoApplyRoutine === "1" || applyTop,
        log_suggestions: logSugg,
        nudge_if_neutral: nudgeFlag === "1",
      };
      const s0 = snap ?? (await fetchSnapshotFast()); if (s0) setRoutineBefore({ ...s0 });
      const r = await fetch(`${base}/optimize/routine`, { method: "POST", headers: postHeaders, body: JSON.stringify(body) });
      if (!r.ok) throw new Error(`/optimize/routine ${r.status}`);
      const j: RoutineResp = await r.json();
      setRoutineRaw(j);
      const created = pickLatestTimestamp(j);
      const proposed = pickProposal(j);
      setRoutineOut(j);
      if (created) setLatestSeenAt(created);
      fetchLastRuns().catch(()=>{});
      if (j?.actuation?.after) {
        const after = j.actuation.after as Snapshot;
        setSnap(after); setRoutineAfter({ ...after }); await fetchTrends();
      }
    } catch (e:any) { setErrorMsg(e?.message || "Run routine failed"); }
  }, [base, postHeaders, o2Min, o2Max, applyTop, logSugg, nudgeFlag, autoApplyRoutine, snap, fetchSnapshotFast, fetchTrends, fetchLastRuns]);

  const applyRoutineProposal = useCallback(async () => {
    const proposed = pickProposal(routineOut);
    if (!proposed) return;
    try {
      const r = await fetch(`${base}/actuate/apply_stage`, { method: "POST", headers: postHeaders, body: JSON.stringify({ proposed_setpoints: proposed, mode: "routine" }) });
      if (!r.ok) throw new Error(`/actuate/apply_stage ${r.status}`);
      const j: ApplyResp = await r.json();
      const after: Snapshot | null = (j.after as Snapshot) || (await fetchSnapshotFast());
      if (after) { setSnap(after); setRoutineAfter({ ...after }); await fetchTrends(); }
      fetchLastRuns().catch(()=>{});
    } catch (e:any) { setErrorMsg(e?.message || "Apply failed"); }
  }, [base, postHeaders, routineOut, fetchSnapshotFast, fetchTrends, fetchLastRuns]);

  const rejectRoutine = useCallback(() => {
    setRoutineOut(null); setRoutineBefore(null); setRoutineAfter(null);
  }, []);

  /* Load planning */
  const [loadMode, setLoadMode] = useState<"pct"|"abs"|"target">("pct");
  const [steps, setSteps] = useState("3");
  const [direction, setDirection] = useState<"up"|"down">("up");
  const [val, setVal] = useState("8");

  const runLoad = useCallback(async () => {
    if (!base) return;
    try {
      const body: any = { steps: Number(steps)||3, direction };
      if (loadMode==="pct") body.delta_pct = Number(val);
      if (loadMode==="abs") body.delta_abs = Number(val);
      if (loadMode==="target") body.target_tph = Number(val);
      const r = await fetch(`${base}/optimize/load`, { method: "POST", headers: postHeaders, body: JSON.stringify(body) });
      if (!r.ok) throw new Error(`/optimize/load ${r.status}`);
      const j: LoadResp = await r.json(); setLoadOut(j);
      const s0 = snap ?? (await fetchSnapshotFast()); if (s0) setLoadBefore({ ...s0 }); setLoadAfter(null);
    } catch (e:any) { setErrorMsg(e?.message || "Create plan failed"); }
  }, [base, postHeaders, steps, direction, val, loadMode, fetchSnapshotFast, snap]);

  const parseStepDwells = useCallback((n: number) => {
    const parts = stepDwellCsv.split(",").map(s=>Number(s.trim())).filter(x=>Number.isFinite(x)&&x>=0);
    if (parts.length===0) return Array(n).fill(20);
    return Array.from({length:n}, (_,i)=>parts[i] ?? parts[0]);
  }, [stepDwellCsv]);

  const applyStage = useCallback(async (i: number) => {
    if (!loadOut) return;
    try {
      const r = await fetch(`${base}/actuate/apply_stage`, {
        method: "POST", headers: postHeaders,
        body: JSON.stringify({ stage: loadOut.stages[i], mode: loadOut.mode, plan_id: loadOut.plan_id, stage_index: i })
      });
      if (!r.ok) throw new Error(`/actuate/apply_stage ${r.status}`);
      const j: ApplyResp = await r.json();
      const after: Snapshot | null = (j.after as Snapshot) || (await fetchSnapshotFast());
      if (after) {
        setSnap(after);
        if (i === loadOut.stages.length - 1) setLoadAfter({ ...after });
        await fetchTrends();
      }
      fetchLastRuns().catch(()=>{});
    } catch (e:any) { setErrorMsg(e?.message || `Apply stage ${i+1} failed`); }
  }, [base, postHeaders, loadOut, fetchSnapshotFast, fetchTrends, fetchLastRuns]);

  const sleep = (ms:number)=>new Promise(res=>setTimeout(res,ms));
  const applyAllStages = useCallback( async () => {
    if (!loadOut) return;
    if (!loadBefore) { const s0 = snap ?? (await fetchSnapshotFast()); if (s0) setLoadBefore({ ...s0 }); }
    const dwells = parseStepDwells(loadOut.stages.length);
    for (let i=0;i<loadOut.stages.length;i++){ await applyStage(i); await sleep(Math.max(0,(dwells[i]??0)*1000)); }
    if (!loadAfter) { const s1 = await fetchSnapshotFast(); if (s1) setLoadAfter({ ...s1 }); }
  }, [loadOut, applyStage, loadBefore, fetchSnapshotFast, snap, loadAfter, parseStepDwells]);

  /* deltas */
  const finalDeltaLoad = useMemo(() => {
    if (!loadBefore || !loadAfter) return null;
    const keys: (keyof Snapshot)[] = [
      "production_tph","o2_percent","specific_power_kwh_per_ton","kiln_feed_tph","separator_dp_pa","id_fan_flow_Nm3_h","cooler_airflow_Nm3_h","kiln_speed_rpm",
    ];
    return keys.map((k) => {
      const b = Number((loadBefore as any)[k]); const a = Number((loadAfter as any)[k]);
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
      const b = Number((routineBefore as any)[k]); const a = Number((routineAfter as any)[k]);
      const d = Number.isFinite(b) && Number.isFinite(a) ? a - b : undefined;
      const p = Number.isFinite(b) && b !== 0 && Number.isFinite(a) ? ((a - b) / b) * 100 : undefined;
      return { k, before: b, after: a, delta: d, pct: p };
    });
  }, [routineBefore, routineAfter]);

  /* responsive chart */
  const chartBoxRef = useRef<HTMLDivElement | null>(null);
  const [chartBoxSize, setChartBoxSize] = useState({ w: 0, h: 0 });
  useEffect(() => {
    if (!chartBoxRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) setChartBoxSize({ w: Math.floor(e.contentRect.width), h: 224 });
    });
    ro.observe(chartBoxRef.current); return () => ro.disconnect();
  }, []);

  const kpi = snap;
  const chartData: TrendPoint[] = trends.length ? trends : history;
  const nextLabel = useMemo(() => {
    if (countdown == null) return "-";
    const m = Math.floor(countdown/60); const s = countdown%60;
    return `${m}:${String(s).padStart(2,"0")} to next routine`;
  }, [countdown]);

  const suggestionLines = useMemo(() => {
    const proposed = pickProposal(routineOut);
    if (!snap || !proposed) return [];
    const out: string[] = [];
    for (const [lever, p] of Object.entries(proposed)) {
      const cur = Number((snap as any)[lever]); const prop = Number(p);
      const pct = Number.isFinite(cur) && cur !== 0 && Number.isFinite(prop) ? ((prop - cur) / cur) * 100 : undefined;
      const base =
        pct == null ? `${lever}: set to ${fmt(prop)}`
        : pct > 0 ? `Increase ${lever} by ~${fmt(Math.abs(pct),1)}% to ${fmt(prop)}`
        : pct < 0 ? `Reduce ${lever} by ~${fmt(Math.abs(pct),1)}% to ${fmt(prop)}`
        : `Hold ${lever} at ${fmt(prop)}`;
      const deltaAbs = Number.isFinite(cur) && Number.isFinite(prop) ? prop - cur : undefined;
      out.push(base + (deltaAbs !== undefined ? ` (Δabs ${deltaAbs >= 0 ? "+" : ""}${fmt(deltaAbs)})` : ""));
    }
    return out;
  }, [snap, routineOut]);

  /* UI */
  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <header className="sticky top-0 z-10 backdrop-blur bg-white/70 border-b border-slate-200">
        <div className="mx-auto max-w-7xl px-4 py-3 flex items-center gap-3">
          <div className="text-xl font-semibold">Plant Agent – Dashboard</div>
          <div className="ml-auto flex items-center gap-2 text-sm">
            <Chip tone="slate">ver {ver}</Chip>
            <Chip tone={health==="200"?"green":"rose"}>health {health}</Chip>
            <Chip tone="indigo">{lastRuns?.last_cron_routine ? `last routine: ${new Date(lastRuns.last_cron_routine).toLocaleString()}` : "last routine: -"}</Chip>
            <Chip tone="amber">{nextLabel}</Chip>
            <label className="ml-2 inline-flex items-center gap-1 text-xs cursor-pointer">
              <input type="checkbox" checked={getDebug()} onChange={(e)=>{ setDebug(e.target.checked); }} /> Debug
            </label>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl p-4 space-y-6">
        {/* Connection */}
        <section className="card">
          <div className="flex flex-col lg:flex-row gap-3 lg:items-end">
            <div className="flex-1">
              <label className="text-xs text-slate-500">API Base URL</label>
              <input value={baseRaw} onChange={(e)=>setBaseRaw(e.target.value)} placeholder="https://<cloud-run-url>" className="w-full mt-1 px-3 py-2 border rounded-xl" />
              <div className="mt-1 text-[11px] text-slate-500">Using: <span className="font-mono">{base || "—"}</span></div>
            </div>
            <div className="flex-1">
              <label className="text-xs text-slate-500">ID Token (optional)</label>
              <input value={token} onChange={(e)=>setToken(e.target.value)} placeholder="paste gcloud-issued ID token" className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <button onClick={()=>setBaseRaw(RECOMMENDED_HOST)} className="btn-outline">Use recommended host</button>
            <button onClick={fetchHealth} className="btn-outline" disabled={!base}>Check</button>
          </div>
          <div className="mt-3 flex items-center gap-3 text-sm flex-wrap">
            <label className="inline-flex items-center gap-2">
              <input type="checkbox" checked={autoPoll==="1"} onChange={(e)=>setAutoPoll(e.target.checked?"1":"0")} /> Auto-refresh snapshot
            </label>
            <div className="inline-flex items-center gap-2">
              <span className="text-xs text-slate-500">Trend source</span>
              <select value={trendSource} onChange={(e)=>setTrendSource(e.target.value)} className="px-2 py-1 border rounded-xl text-xs">
                <option value="auto">auto</option>
                <option value="bq">bq</option>
              </select>
              <span className="text-xs text-slate-500">from <span className="font-mono">{trendEndpoint}</span></span>
            </div>
            <button onClick={()=>{ fetchSnapshot(); fetchTrends(); fetchLastRuns(); fetchRoutineLatest(); getMetrics(); }} className="btn-outline">Refresh now</button>
            {errorMsg ? <span className="text-rose-600">• {errorMsg}</span> : null}
          </div>
          {countdown == null && (
            <div className="banner-info mt-3">
              {countdownCause || "Countdown unavailable."} We try both GET and POST for last-run info.
            </div>
          )}
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
            <div key={idx} className="tile">
              <div className="text-xs text-slate-500">{t.label}</div>
              <div className="text-2xl font-semibold mt-1">{fmt(t.val, 3)}</div>
            </div>
          ))}
        </section>

        {/* Suggestions */}
        <section className="card">
          <div className="flex items-center justify-between">
            <div className="font-semibold">Suggestions</div>
            <div className="text-xs text-slate-500">
              latest routine run: {pickLatestTimestamp(routineOut) ? new Date(pickLatestTimestamp(routineOut) as string).toLocaleString() : "-"}
              {routineOut?.suggestion_id ? <span className="ml-2">• id: <span className="font-mono">{routineOut.suggestion_id}</span></span> : null}
              {routineOut?.reason ? <span className="ml-2">• {routineOut.reason}</span> : null}
              {routineOut?.flags_effective && (
                <span className="ml-2">
                  • flags: apply_top=<b>{String(routineOut.flags_effective.apply_top)}</b>,
                  log=<b>{String(routineOut.flags_effective.log_suggestions)}</b>,
                  nudge=<b>{String(routineOut.flags_effective.nudge_if_neutral)}</b>
                </span>
              )}
            </div>
          </div>

          {suggestionLines.length ? (
            <ul className="mt-3 list-disc pl-6 text-sm space-y-1">
              {suggestionLines.map((s,i)=><li key={i}>{s}</li>)}
            </ul>
          ) : (
            <div className="mt-3 text-sm text-slate-500">
              {countdown !== null ? "Waiting for the next cron suggestion… we’ll auto-fetch around the scheduled time." : "Run a routine optimization to populate suggestions."}
            </div>
          )}

          {/* per-lever audit table */}
          {routineOut?.per_lever && (
            <div className="mt-4 overflow-x-auto">
              <div className="text-sm font-medium mb-1">Per-lever audit</div>
              <table className="min-w-full text-xs">
                <thead>
                  <tr className="text-left text-slate-500 border-b">
                    <th className="py-2 pr-4">Lever</th>
                    <th className="py-2 pr-4">Decision</th>
                    <th className="py-2 pr-4">Rule</th>
                    <th className="py-2 pr-4">Current</th>
                    <th className="py-2 pr-4">Proposed</th>
                    <th className="py-2 pr-4">Δabs</th>
                    <th className="py-2 pr-4">Δ%</th>
                    <th className="py-2 pr-4">Bounds</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(routineOut.per_lever).map(([k, v]) => (
                    <tr key={k} className="border-b last:border-0">
                      <td className="py-2 pr-4 font-medium">{k}</td>
                      <td className="py-2 pr-4">{v.decision}</td>
                      <td className="py-2 pr-4">{v.rule ?? "—"}</td>
                      <td className="py-2 pr-4 font-mono">{fmt(v.current)}</td>
                      <td className="py-2 pr-4 font-mono">{v.proposed != null ? fmt(v.proposed) : (v.proposed_before_filter != null ? fmt(v.proposed_before_filter) : "—")}</td>
                      <td className="py-2 pr-4 font-mono">{v.delta_abs != null ? ((v.delta_abs >= 0 ? "+" : "") + fmt(v.delta_abs)) : "—"}</td>
                      <td className="py-2 pr-4 font-mono">{v.delta_pct != null ? ((v.delta_pct*100 >= 0 ? "+" : "") + fmt(v.delta_pct*100, 2) + "%") : "—"}</td>
                      <td className="py-2 pr-4">{v.bounds.min ?? "−∞"}…{v.bounds.max ?? "+∞"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {routineOut.reason_detail && (
                <div className="mt-2 text-[11px] text-slate-600">Details: {routineOut.reason_detail}</div>
              )}
            </div>
          )}

          <div className="mt-3 flex flex-wrap gap-2 items-center">
            <button onClick={applyRoutineProposal} disabled={!pickProposal(routineOut)} className="btn-primary">Accept & Apply</button>
            <button onClick={rejectRoutine} className="btn-danger">Reject</button>
            {routineOut?.applied ? <Chip tone="green">Auto-applied ✓</Chip> : <Chip tone="slate">Not auto-applied</Chip>}
            {routineOut?.actuation?.bq_log?.insert_error && (
              <span className="text-rose-600 text-xs">BQ: {routineOut.actuation.bq_log.insert_error}</span>
            )}
          </div>
        </section>

        {/* Trends + Snapshot */}
        <section className="grid md:grid-cols-2 gap-4">
          <div className="card">
            <div className="font-semibold mb-1">Important Trends (last 2 hours)</div>
            <div className="text-xs text-slate-500 mb-2">
              {chartData.length ? `Loaded ${chartData.length} points from ${trendEndpoint}` : "Waiting for data…"}
            </div>
            <div ref={chartBoxRef} className="h-56 w-full">
              {(chartData.length && chartBoxSize.w>0) ? (
                <LineChart width={chartBoxSize.w} height={chartBoxSize.h} data={chartData} margin={{ top: 8, right: 32, left: 8, bottom: 8 }}>
                  <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
                  <XAxis dataKey="t" tick={{ fill: "#334155", fontSize: 12 }} axisLine={{ stroke: "#94a3b8" }} tickLine={{ stroke: "#94a3b8" }} />
                  <YAxis yAxisId="left" domain={["auto","auto"]} tick={{ fill: "#334155", fontSize: 12 }} axisLine={{ stroke: "#94a3b8" }} tickLine={{ stroke: "#94a3b8" }} />
                  <YAxis yAxisId="right" orientation="right" domain={["auto","auto"]} tick={{ fill: "#334155", fontSize: 12 }} axisLine={{ stroke: "#94a3b8" }} tickLine={{ stroke: "#94a3b8" }} />
                  <Tooltip /><Legend wrapperStyle={{ paddingTop: 8 }} />
                  <Line yAxisId="left" type="monotone" dataKey="production_tph" name="Production (tph)" stroke="#0ea5e9" strokeWidth={2} dot={{ r: 2 }} isAnimationActive={false} connectNulls />
                  <Line yAxisId="right" type="monotone" dataKey="o2_percent" name="O₂ (%)" stroke="#22c55e" strokeWidth={2} dot={{ r: 2 }} isAnimationActive={false} connectNulls />
                  <Line yAxisId="right" type="monotone" dataKey="specific_power_kwh_per_ton" name="Specific Power (kWh/t)" stroke="#f59e0b" strokeWidth={2} dot={{ r: 2 }} isAnimationActive={false} connectNulls />
                </LineChart>
              ) : (
                <div className="h-full grid place-items-center text-slate-400 text-sm">No data to chart</div>
              )}
            </div>
          </div>

          <div className="card">
            <div className="font-semibold mb-2">Current Snapshot</div>
            {snap ? (
              <div className="grid grid-cols-2 gap-2 text-sm">
                {Object.entries(snap).map(([k,v])=>(
                  <div key={k} className="flex justify-between border-b py-1">
                    <span className="text-slate-500">{k}</span>
                    <span className="font-mono">{fmt(Number(v))}</span>
                  </div>
                ))}
              </div>
            ) : <div className="text-slate-500 text-sm">No snapshot yet.</div>}
          </div>
        </section>

        {/* Routine Controls */}
        <section className="card">
          <div className="flex items-center justify-between">
            <div className="font-semibold">Routine Optimization</div>
            <div className="text-xs text-slate-500">Manual run mirrors cron defaults unless toggles override</div>
          </div>
          <div className="mt-3 grid md:grid-cols-8 gap-3 items-end">
            <div>
              <label className="text-xs text-slate-500">O₂ min</label>
              <input value={o2Min} onChange={(e)=>setO2Min(e.target.value)} className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <div>
              <label className="text-xs text-slate-500">O₂ max</label>
              <input value={o2Max} onChange={(e)=>setO2Max(e.target.value)} className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <label className="inline-flex items-center gap-2 text-sm">
              <input type="checkbox" checked={logSugg} onChange={(e)=>setLogSugg(e.target.checked)} /> Log suggestions
            </label>
            <label className="inline-flex items-center gap-2 text-sm">
              <input type="checkbox" checked={nudgeFlag==="1"} onChange={(e)=>setNudgeFlag(e.target.checked ? "1" : "0")} /> Nudge if neutral
            </label>
            <label className="inline-flex items-center gap-2 text-sm">
              <input type="checkbox" checked={applyTop} onChange={(e)=>setApplyTop(e.target.checked)} /> Apply top (this click)
            </label>
            <label className="inline-flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={autoApplyRoutine==="1"}
                onChange={(e)=>setAutoApplyRoutine(e.target.checked ? "1" : "0")}
              /> Auto-apply routine run (UI default)
            </label>
            <button onClick={runRoutine} className="btn-secondary" disabled={!base}>Run routine</button>
            <div className="text-xs text-slate-500 col-span-2">
              Next: {lastRuns?.next_cron_eta ? new Date(lastRuns.next_cron_eta).toLocaleTimeString() : "-"} • Period: {lastRuns?.sched_period_sec ?? "-"}s
            </div>
          </div>
        </section>

        {/* Load Planning */}
        <section className="card">
          <div className="flex items-center justify-between">
            <div className="font-semibold">Load Planning</div>
            <div className="text-xs text-slate-500">latest plan: {loadOut?.created_at ? new Date(loadOut.created_at).toLocaleString() : "-" } • id: {loadOut?.plan_id || "-"}</div>
          </div>
          <div className="mt-3 grid md:grid-cols-7 gap-3 items-end">
            <div>
              <label className="text-xs text-slate-500">Approach</label>
              <select value={loadMode} onChange={(e)=>setLoadMode(e.target.value as any)} className="w-full mt-1 px-3 py-2 border rounded-xl">
                <option value="pct">delta_pct %</option>
                <option value="abs">delta_abs (tph)</option>
                <option value="target">target_tph</option>
              </select>
            </div>
            <div>
              <label className="text-xs text-slate-500">Value</label>
              <input value={val} onChange={(e)=>setVal(e.target.value)} className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <div>
              <label className="text-xs text-slate-500">Steps</label>
              <input value={steps} onChange={(e)=>setSteps(e.target.value)} className="w-full mt-1 px-3 py-2 border rounded-xl" />
            </div>
            <div>
              <label className="text-xs text-slate-500">Direction</label>
              <select value={direction} onChange={(e)=>setDirection(e.target.value as any)} className="w-full mt-1 px-3 py-2 border rounded-xl">
                <option value="up">up</option>
                <option value="down">down</option>
              </select>
            </div>
            <div className="col-span-2">
              <label className="text-xs text-slate-500">Step dwell seconds (CSV; per-stage)</label>
              <input value={stepDwellCsv} onChange={(e)=>setStepDwellCsv(e.target.value)} className="w-full mt-1 px-3 py-2 border rounded-xl font-mono" />
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
                  <button onClick={applyAllStages} className="mt-3 btn-primary">Accept & Apply All</button>
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
          <section className="card border-emerald-200">
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
                    <tr key={String(r.k)} className="border-b last:border-0">
                      <td className="py-2 pr-4 font-medium">{String(r.k)}</td>
                      <td className="py-2 pr-4 font-mono">{fmt((r as any).before)}</td>
                      <td className="py-2 pr-4 font-mono">{fmt((r as any).after)}</td>
                      <td className={cls("py-2 pr-4 font-mono", ((r as any).delta ?? 0) >= 0 ? "text-emerald-700" : "text-rose-700")}>
                        {(r as any).delta !== undefined ? (((r as any).delta >= 0 ? "+" : "") + fmt((r as any).delta)) : "-"}
                      </td>
                      <td className={cls("py-2 pr-4 font-mono", ((r as any).pct ?? 0) >= 0 ? "text-emerald-700" : "text-rose-700")}>
                        {(r as any).pct !== undefined ? (((r as any).pct >= 0 ? "+" : "") + fmt((r as any).pct, 2) + "%") : "-"}
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
          <section className="card border-emerald-200">
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
                    <tr key={String(r.k)} className="border-b last:border-0">
                      <td className="py-2 pr-4 font-medium">{String(r.k)}</td>
                      <td className="py-2 pr-4 font-mono">{fmt((r as any).before)}</td>
                      <td className="py-2 pr-4 font-mono">{fmt((r as any).after)}</td>
                      <td className={cls("py-2 pr-4 font-mono", ((r as any).delta ?? 0) >= 0 ? "text-emerald-700" : "text-rose-700")}>
                        {(r as any).delta !== undefined ? (((r as any).delta >= 0 ? "+" : "") + fmt((r as any).delta)) : "-"}
                      </td>
                      <td className={cls("py-2 pr-4 font-mono", ((r as any).pct ?? 0) >= 0 ? "text-emerald-700" : "text-rose-700")}>
                        {(r as any).pct !== undefined ? (((r as any).pct >= 0 ? "+" : "") + fmt((r as any).pct, 2) + "%") : "-"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {/* Metrics */}
        <MetricsBlock base={base} getHeaders={getHeaders} metrics={metrics} metricsErr={metricsErr} onRefresh={getMetrics} />

        {/* Debug */}
        {getDebug() && (
          <section className="debug-card">
            <div className="font-semibold mb-2">Debug</div>
            <div className="grid md:grid-cols-3 gap-4 text-xs">
              <div>
                <div className="opacity-80 mb-1">last_runs</div>
                <pre className="debug-pre">{JSON.stringify(lastRuns,null,2)}</pre>
              </div>
              <div>
                <div className="opacity-80 mb-1">/routine/latest (raw)</div>
                <pre className="debug-pre">{JSON.stringify(routineRaw ?? routineOut,null,2)}</pre>
              </div>
              <div>
                <div className="opacity-80 mb-1">countdown</div>
                <div className="font-mono text-lg">{countdown ?? "-"}</div>
                {countdownCause && <div className="mt-1 text-amber-300">cause: {countdownCause}</div>}
              </div>
            </div>
            <div className="mt-3 flex gap-2">
              <button className="btn-outline" onClick={()=>fetchTrends()}>Re-fetch trends</button>
              <button className="btn-outline" onClick={()=>{ disableLastRunsRef.current=false; fetchLastRuns(); }}>Re-fetch last_runs</button>
              <button className="btn-outline" onClick={()=>fetchRoutineLatest()}>Fetch /routine/latest now</button>
              <button className="btn-outline" onClick={()=>getMetrics()}>Refresh metrics</button>
            </div>
          </section>
        )}
      </main>

      {/* Diagnostics footer */}
      <footer className="mt-6 border-t border-slate-200 bg-white/70">
        <div className="mx-auto max-w-7xl px-4 py-3 text-xs text-slate-600 flex flex-wrap gap-x-4 gap-y-1">
          <span>Sched period: <b>{metrics?.sched_period_sec ?? "-"}</b>s</span>
          <span>Apply enabled: <b>{String(metrics?.apply_enabled)}</b></span>
          <span>SPower: mode=<b>{metrics?.spower?.mode}</b>, tol=<b>{metrics?.spower?.tol}</b></span>
          <span>Thresholds: pct=<b>{metrics?.thresholds?.MIN_PCT_DELTA}</b>, idfan=<b>{metrics?.thresholds?.MIN_ABS_ID_FAN}</b>, cooler=<b>{metrics?.thresholds?.MIN_ABS_COOLER}</b></span>
          <span>Cron defaults: apply_top=<b>{String(metrics?.cron_defaults?.CRON_APPLY_TOP)}</b>, nudge=<b>{String(metrics?.cron_defaults?.CRON_NUDGE_IF_NEUTRAL)}</b>, log=<b>{String(metrics?.cron_defaults?.CRON_LOG_SUGGESTIONS)}</b></span>
        </div>
      </footer>
    </div>
  );
}

/* Small chip + styles */
function Chip({ children, tone="slate" }: { children: React.ReactNode; tone?: "slate" | "green" | "rose" | "amber" | "indigo" }) {
  const map: Record<string, string> = {
    slate: "chip chip-slate", green: "chip chip-green", rose: "chip chip-rose", amber: "chip chip-amber", indigo: "chip chip-indigo",
  };
  return <span className={map[tone]}>{children}</span>;
}

/* Metrics sub-block (diagnostics) */
function MetricsBlock({ base, getHeaders, metrics, metricsErr, onRefresh }: { base: string; getHeaders: Record<string,string>; metrics:any; metricsErr:string; onRefresh:()=>void; }) {
  return (
    <section className="card">
      <div className="flex items-center justify-between">
        <div className="font-semibold">Metrics</div>
        <button onClick={onRefresh} className="btn-outline">Refresh</button>
      </div>
      {metricsErr && <div className="text-rose-600 text-sm mt-2">{metricsErr}</div>}
      <pre className="text-xs bg-slate-50 border rounded-2xl p-3 overflow-auto mt-2">{JSON.stringify(metrics, null, 2)}</pre>
    </section>
  );
}

/* ======= minimal styles (Tailwind-friendly) ======= */
/* In your global.css (or keep utility classes). This block assumes you already use Tailwind.
.card { @apply bg-white border border-slate-200 rounded-2xl p-4; }
.tile { @apply bg-white border border-slate-200 rounded-2xl p-3; }
.btn-outline { @apply px-3 py-2 border rounded-xl text-sm hover:bg-slate-50; }
.btn-secondary { @apply px-3 py-2 rounded-xl text-sm bg-slate-800 text-white hover:bg-slate-700; }
.btn-indigo { @apply px-3 py-2 rounded-xl text-sm bg-indigo-600 text-white hover:bg-indigo-500; }
.btn-primary { @apply px-3 py-2 rounded-xl text-sm bg-emerald-600 text-white hover:bg-emerald-500; }
.btn-danger { @apply px-3 py-2 rounded-xl text-sm bg-rose-600 text-white hover:bg-rose-500; }
.banner-info { @apply text-xs text-slate-700 bg-slate-100 border border-slate-200 rounded-xl px-3 py-2; }
.debug-card { @apply bg-slate-900 text-slate-100 rounded-2xl p-4; }
.debug-pre { @apply bg-slate-800 border border-slate-700 rounded-xl p-3 overflow-auto; }
.chip { @apply inline-flex items-center px-2 py-0.5 rounded-full text-[11px] border; }
.chip-slate { @apply bg-slate-50 text-slate-700 border-slate-200; }
.chip-green { @apply bg-emerald-50 text-emerald-700 border-emerald-200; }
.chip-rose { @apply bg-rose-50 text-rose-700 border-rose-200; }
.chip-amber { @apply bg-amber-50 text-amber-700 border-amber-200; }
.chip-indigo { @apply bg-indigo-50 text-indigo-700 border-indigo-200; } */
