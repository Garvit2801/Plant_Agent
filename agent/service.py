# service.py
from __future__ import annotations

import os
import json
import logging
import glob
import threading
import time
import uuid
import math
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import yaml
import pandas as pd
import numpy as np
import datetime
from fastapi import FastAPI, HTTPException, Body, Query, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# ------------------------------------------------------------
# Optional planner import (fallbacks provided if not available)
# ------------------------------------------------------------
try:
    from agent.planner import build_stage_plan, propose_actions, build_ui_payload
except Exception:
    def _pct_step(current: float, target: float, step_pct: float) -> float:
        return current + (target - current) * (step_pct / 100.0)

    def _abs_step(current: float, target: float, step_abs: float) -> float:
        delta = target - current
        if abs(delta) <= step_abs:
            return target
        return current + (step_abs if delta > 0 else -step_abs)

    def propose_actions(now: Dict[str, Any], recipe: Dict[str, Any], levers: Dict[str, Any]) -> List[Dict[str, Any]]:
        actions = {}
        for lever, cfg in levers.items():
            if lever in now and lever in recipe:
                if "step_pct" in cfg:
                    actions[lever] = round(_pct_step(now[lever], recipe[lever], cfg["step_pct"]), 3)
                elif "step_abs" in cfg:
                    actions[lever] = round(_abs_step(now[lever], recipe[lever], cfg["step_abs"]), 3)
        return [actions] if actions else []

    def build_stage_plan(now: Dict[str, Any], target: Dict[str, Any], levers: Dict[str, Any], stages_max: int = 4) -> List[Dict[str, Any]]:
        stages: List[Dict[str, Any]] = []
        current = dict(now)
        for s in range(1, stages_max + 1):
            stage_set = {}
            moved = False
            for lever, cfg in levers.items():
                if lever not in target or lever not in current:
                    continue
                lo, hi = cfg.get("min", -1e12), cfg.get("max", 1e12)
                if "step_pct" in cfg:
                    nxt = _pct_step(current[lever], target[lever], cfg["step_pct"])
                elif "step_abs" in cfg:
                    nxt = _abs_step(current[lever], target[lever], cfg["step_abs"])
                else:
                    continue
                nxt = max(lo, min(hi, nxt))
                if abs(nxt - current[lever]) > 1e-6:
                    stage_set[lever] = round(nxt, 3)
                    moved = True
            if not moved:
                break
            checks = [
                "O2 in [2.5, 4.5] %",
                "CO < 180 ppm",
                "Bagfilter ΔP < 1800 Pa",
                "Mill outlet temp 90–120 °C",
                "Drive load factor < 95%",
            ]
            stages.append({"name": f"Stage {s}", "setpoints": stage_set, "checks": checks})
            current.update(stage_set)
        return stages

    def build_ui_payload(mode: str, now: Dict[str, Any], proposal: Any, pred: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "mode": mode,
            "current": now,
            "predicted_after": pred,
            "actions": {"apply_stage": True, "apply_all": mode.startswith("load_"), "rollback": True},
        }
        if isinstance(proposal, list):
            payload["stages"] = proposal
        else:
            payload["proposed_setpoints"] = proposal or {}
        return payload


# -------------------------
# Configuration / constants
# -------------------------
DATA_FILE = os.getenv("DATA_FILE", "data/cement_240TPD_KPI_mockup_v2.xlsx")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
PORT = int(os.getenv("PORT", "8080"))

USE_MOCK = int(os.getenv("USE_MOCK", "1"))
MOCK_TICK_SEC = float(os.getenv("MOCK_TICK_SEC", "5"))
APPLY_ENABLED = int(os.getenv("APPLY_ENABLED", "1"))
AUTO_INGEST_ON_APPLY = int(os.getenv("AUTO_INGEST_ON_APPLY", "1"))
SCHED_PERIOD_SEC = int(os.getenv("SCHED_PERIOD_SEC", "300"))  # default 5 min

# --- routine min-change thresholds (envs) ---
MIN_PCT_DELTA = float(os.getenv("MIN_PCT_DELTA", "0.0"))           # fraction, e.g. 0.001 = 0.1%
MIN_ABS_ID_FAN = float(os.getenv("MIN_ABS_ID_FAN", "0.0"))         # Nm3/h
MIN_ABS_COOLER = float(os.getenv("MIN_ABS_COOLER", "0.0"))         # Nm3/h

# Cron defaults (used only as *inputs* for manual runs; cron enforces apply_top=False)
CRON_APPLY_TOP = os.getenv("CRON_APPLY_TOP", "false").lower() in ("1","true","yes")
CRON_NUDGE_IF_NEUTRAL = os.getenv("CRON_NUDGE_IF_NEUTRAL", "true").lower() in ("1","true","yes")
CRON_LOG_SUGGESTIONS = os.getenv("CRON_LOG_SUGGESTIONS", "true").lower() in ("1","true","yes")

# Project (prefer explicit env)
PROJECT_ID = (
    os.getenv("PROJECT_ID")
    or os.getenv("GOOGLE_CLOUD_PROJECT")
    or os.getenv("GCP_PROJECT")
    or ""
)

BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-south2")
MODEL_NAME = os.getenv("BQ_MODEL_NAME", "spower_reg")

# NEW: dataset/envs used for views
VIEW_DATASET = os.getenv("VIEW_DATASET", "plant_ops")
RAW_DATASET = os.getenv("RAW_DATASET", "plant_ops")
RAW_TABLE = os.getenv("RAW_TABLE", "raw_signals")

# Table envs
BQ_SNAPSHOTS_TABLE_ENV = os.getenv("BQ_SNAPSHOTS_TABLE")
BQ_SNAPSHOTS_VIEW_ENV = os.getenv("BQ_SNAPSHOTS_VIEW")
BQ_PLANS_TABLE_ENV       = os.getenv("BQ_PLANS_TABLE")
BQ_ACTS_TABLE_ENV        = os.getenv("BQ_ACTUATIONS_TABLE")
BQ_ROUTINE_TABLE_ENV     = os.getenv("BQ_ROUTINE_TABLE")
BQ_SUGGESTIONS_TABLE_ENV = os.getenv("BQ_SUGGESTIONS_TABLE")

# KPI predictions tables
BQ_KPI_PRED_LATEST_ENV = os.getenv("BQ_KPI_PRED_LATEST_TABLE")
BQ_KPI_PRED_FUTURE_ENV = os.getenv("BQ_KPI_PRED_FUTURE_TABLE")

# SPOWER controls
SPOWER_MODE = os.getenv("SPOWER_MODE", "static").lower()  # "static" or "dynamic"
SPOWER_TOL = float(os.getenv("SPOWER_TOL", "0.005"))       # 0.5% relative tolerance
LOG_PHYSICS = os.getenv("LOG_PHYSICS", "0") in ("1","true","yes")

# Condition Monitoring KPIs
# schema: ts TIMESTAMP, kpi STRING, value FLOAT64, aux JSON, source STRING
BQ_CM_KPI_TABLE_ENV = os.getenv("BQ_CM_KPI_TABLE")  # e.g. myproj.plant_ops.cm_kpis

# Forecasting toggles
FORECAST_PM_ENABLE = os.getenv("FORECAST_PM_ENABLE", "1") in ("1", "true", "yes")
FORECAST_PM_INTERVAL_SEC = int(os.getenv("FORECAST_PM_INTERVAL_SEC", "60"))
FORECAST_PM_HORIZON_MIN = int(os.getenv("FORECAST_PM_HORIZON_MIN", "60"))
FORECAST_PM_STEP_MIN = int(os.getenv("FORECAST_PM_STEP_MIN", "5"))
PM_KPIS = [
    "specific_power_kwh_per_ton",
    "vibration_axial_mm_s",
    "vibration_radial_mm_s",
    "motor_current_a",
    "motor_current_b",
    "motor_current_c",
    "bearing_temp_C",
    "suction_temp_C",
]

# -------------------------
# App & CORS
# -------------------------
app = FastAPI(title="Plant Agent API", version=SERVICE_VERSION)

@app.middleware("http")
async def stamp_and_log(request: Request, call_next):
    logging.info(
        "REQ method=%s path=%s origin=%s",
        request.method, request.url.path, request.headers.get("origin"),
    )
    resp = await call_next(request)
    resp.headers["X-Service-Version"] = SERVICE_VERSION
    resp.headers["X-Sched-Period-Sec"] = str(SCHED_PERIOD_SEC)
    resp.headers["Vary"] = "Origin"
    return resp

_default_ui_origins = [
    "http://localhost:5173",
    "http://localhost:3000",
    "https://your-ui.example.com",
    "https://garvit2801.github.io",
    "https://garvit2801.github.io/Plant_Agent",
    "https://storage.googleapis.com/my-plant-agent-123456-plant-ui/index.html",
]
_env_ui = [o.strip() for o in os.getenv("UI_ORIGINS", "").split(",") if o.strip()]
UI_ORIGINS = _env_ui or _default_ui_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=UI_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["X-Service-Version", "X-Sched-Period-Sec"],
    max_age=86400,
)

def _origin_allowed(origin: Optional[str]) -> bool:
    return bool(origin and origin in UI_ORIGINS)

def _cors_hdrs_for(origin: Optional[str]) -> Dict[str, str]:
    if _origin_allowed(origin):
        return {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Max-Age": "86400",
            "Vary": "Origin",
        }
    return {}

@app.options("/{path:path}")
async def options_any(path: str, request: Request):
    origin = request.headers.get("origin")
    return Response(status_code=204, headers=_cors_hdrs_for(origin))

@app.options("/cron/routine")
def options_cron_routine(request: Request):
    origin = request.headers.get("origin")
    return Response(status_code=204, headers=_cors_hdrs_for(origin) | {"Allow": "POST, OPTIONS"})

@app.options("/optimize/routine")
def options_optimize_routine(request: Request):
    origin = request.headers.get("origin")
    return Response(status_code=204, headers=_cors_hdrs_for(origin) | {"Allow": "POST, OPTIONS"})

# -------------------------
# Robust plant.yaml resolver
# -------------------------
def _resolve_config_path() -> str:
    p = os.getenv("PLANT_CONFIG")
    if p and os.path.exists(p):
        logging.info(f"PLANT_CONFIG env set, using: {p}")
        return p
    candidates = ["/app/config/plant.yaml", "/app/plant.yaml", "config/plant.yaml", "plant.yaml"]
    for c in candidates:
        if os.path.exists(c):
            logging.info(f"Found plant.yaml at: {c}")
            return c
    try:
        root_listing = sorted(glob.glob("/app/**/*", recursive=True))[:50]
        logging.error("plant.yaml not found. Sample /app listing: %s", root_listing)
    except Exception as e:
        logging.error("plant.yaml not found and listing failed: %s", e)
    raise FileNotFoundError("Missing config: plant.yaml")

_config_cache: Dict[str, Any] = {}
_config_mtime: Optional[float] = None
_config_path: Optional[str] = None

def _stat_mtime(path: str) -> Optional[float]:
    try:
        return os.path.getmtime(path)
    except FileNotFoundError:
        return None

def get_config() -> Dict[str, Any]:
    global _config_cache, _config_mtime, _config_path
    resolved = _resolve_config_path()
    mt = _stat_mtime(resolved)
    if not mt:
        raise HTTPException(status_code=500, detail=f"Missing config: {os.path.basename(resolved)}")
    if _config_path != resolved or _config_mtime != mt or not _config_cache:
        with open(resolved, "r") as f:
            _config_cache = yaml.safe_load(f) or {}
        _config_mtime = mt
        _config_path = resolved
        logging.info("Loaded plant config keys: %s", list(_config_cache.keys()))
    return _config_cache

# -------------------------
# Data presence check (Excel)
# -------------------------
def ensure_data_present() -> Tuple[str, List[str]]:
    if not os.path.exists(DATA_FILE):
        return ("missing", [])
    xls = pd.ExcelFile(DATA_FILE)
    return ("ok", xls.sheet_names)

# -------------------------
# Helpers
# -------------------------
def _now_ts() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def predict_specific_power(snapshot: Dict[str, Any]) -> float:
    cur = float(snapshot.get("specific_power_kwh_per_ton", 12.5))
    return round(cur * 0.99, 3)

KPI_KEYS = [
    "production_tph", "kiln_feed_tph", "separator_dp_pa",
    "id_fan_flow_Nm3_h", "cooler_airflow_Nm3_h",
    "kiln_speed_rpm", "o2_percent", "specific_power_kwh_per_ton"
]

def _diff_kpis(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    for k in KPI_KEYS:
        va = a.get(k)
        vb = b.get(k)
        try:
            out[k] = (float(vb) - float(va)) if va is not None and vb is not None else None
        except Exception:
            out[k] = None
    out["ts"] = _now_ts().isoformat()
    return out

def _as_json_string(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)

def _format_suggestion_text(lever: str, current: Optional[float], proposed: Optional[float],
                            delta_pct: Optional[float], cmin: Optional[float], cmax: Optional[float]) -> str:
    def _fmt(x: Optional[float]) -> str:
        try:
            return f"{float(x):.3f}"
        except Exception:
            return "N/A"

    if delta_pct is None or current in (None, 0) or proposed is None:
        base = f"{lever}: set to {_fmt(proposed)}"
    elif delta_pct > 0:
        base = f"Increase {lever} by ~{abs(round(delta_pct,1))}% to {_fmt(proposed)}"
    elif delta_pct < 0:
        base = f"Reduce {lever} by ~{abs(round(delta_pct,1))}% to {_fmt(proposed)}"
    else:
        base = f"Hold {lever} at {_fmt(proposed)}"
    bounds = ""
    if cmin is not None or cmax is not None:
        lo = f"{cmin:.3f}" if cmin is not None else "−∞"
        hi = f"{cmax:.3f}" if cmax is not None else "+∞"
        bounds = f" (bounds {lo}…{hi})"
    return base + bounds

def _md5_short(d: Dict[str, Any]) -> str:
    try:
        s = json.dumps(d, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.md5(s).hexdigest()[:8]
    except Exception:
        return "00000000"

# -------------------------
# Mock plant state & thread (with SPs) + history ring buffer
# -------------------------
_state_lock = threading.Lock()
_STATE: Dict[str, float] = {
    "production_tph": 10.0,
    "kiln_feed_tph": 10.0,
    "separator_dp_pa": 620.0,
    "id_fan_flow_Nm3_h": 150000.0,
    "cooler_airflow_Nm3_h": 220000.0,
    "kiln_speed_rpm": 3.5,
    "o2_percent": 2.6,
    "specific_power_kwh_per_ton": 12.2,
}
_STATE.setdefault("sp", {
    "kiln_feed_tph": _STATE["kiln_feed_tph"],
    "separator_dp_pa": _STATE["separator_dp_pa"],
    "id_fan_flow_Nm3_h": _STATE["id_fan_flow_Nm3_h"],
    "cooler_airflow_Nm3_h": _STATE["cooler_airflow_Nm3_h"],
    "kiln_speed_rpm": _STATE["kiln_speed_rpm"],
})
_STATE["_spower_drivers"] = {
    "production_tph": _STATE["production_tph"],
    "separator_dp_pa_sp": _STATE["sp"]["separator_dp_pa"],
    "o2_percent": _STATE["o2_percent"],
}
# PM variables
_STATE.update({
    "vibration_axial_mm_s": 2.0,
    "vibration_radial_mm_s": 1.8,
    "motor_current_a": 145.0,
    "motor_current_b": 147.0,
    "motor_current_c": 143.0,
    "bearing_temp_C": 58.0,
    "suction_temp_C": 34.0,
})

_HIST = deque(maxlen=int(os.getenv("MOCK_HISTORY_MAX", "2000")))

def _append_history(snapshot: Dict[str, Any]):
    _HIST.append({
        "ts": _now_ts().isoformat(),
        "production_tph": snapshot.get("production_tph"),
        "o2_percent": snapshot.get("o2_percent"),
        "specific_power_kwh_per_ton": snapshot.get("specific_power_kwh_per_ton"),
        # PM signals
        "vibration_axial_mm_s": snapshot.get("vibration_axial_mm_s"),
        "vibration_radial_mm_s": snapshot.get("vibration_radial_mm_s"),
        "motor_current_a": snapshot.get("motor_current_a"),
        "motor_current_b": snapshot.get("motor_current_b"),
        "motor_current_c": snapshot.get("motor_current_c"),
        "bearing_temp_C": snapshot.get("bearing_temp_C"),
        "suction_temp_C": snapshot.get("suction_temp_C"),
    })

def _log_phys(tag: str, **kw):
    if LOG_PHYSICS:
        try:
            logging.info("PHYS %-8s %s", tag, " ".join(f"{k}={kw[k]}" for k in sorted(kw.keys())))
        except Exception:
            pass

def _drivers_changed_enough(state: Dict[str, float], sp: Dict[str, float]) -> Tuple[bool, Dict[str, float]]:
    last = state.get("_spower_drivers") or {}
    cur = {
        "production_tph": float(state.get("production_tph", 0.0)),
        "separator_dp_pa_sp": float(sp.get("separator_dp_pa", state.get("separator_dp_pa", 0.0))),
        "o2_percent": float(state.get("o2_percent", 0.0)),
    }
    def rel_changed(a: float, b: float) -> bool:
        if a == 0 and b == 0:
            return False
        denom = max(1e-6, abs(a))
        return abs(b - a) / denom >= SPOWER_TOL
    changed = (
        rel_changed(last.get("production_tph", cur["production_tph"]), cur["production_tph"]) or
        rel_changed(last.get("separator_dp_pa_sp", cur["separator_dp_pa_sp"]), cur["separator_dp_pa_sp"]) or
        rel_changed(last.get("o2_percent", cur["o2_percent"]), cur["o2_percent"])
    )
    return changed, cur

def _physics_tick(state: Dict[str, float], dt_sec: float) -> None:
    sp = state.get("sp", {})
    if not sp:
        return

    cfg = get_config()
    levers: Dict[str, Any] = cfg.get("levers", {})

    # 1) PVs → SPs
    follow_alpha = min(1.0, dt_sec / 8.0)
    for k in ("kiln_feed_tph", "separator_dp_pa", "id_fan_flow_Nm3_h", "cooler_airflow_Nm3_h", "kiln_speed_rpm"):
        if k in sp:
            lo = levers.get(k, {}).get("min", -1e12)
            hi = levers.get(k, {}).get("max", 1e12)
            target = clamp(float(sp[k]), lo, hi)
            prev = state[k]
            state[k] = prev + follow_alpha * (target - prev)

    # 2) production tracks kiln_feed SP
    tau_prod = 20.0
    prod_alpha = min(1.0, dt_sec / tau_prod)
    desired_prod = float(sp.get("kiln_feed_tph", state["kiln_feed_tph"])) * 1.00
    if not (2.5 <= state["o2_percent"] <= 4.5):
        desired_prod = min(desired_prod, state["production_tph"])
    prod_lo = levers.get("production_tph", {}).get("min", 0.0) or 0.0
    prod_hi = levers.get("production_tph", {}).get("max", 1e12)
    desired_prod = clamp(desired_prod, prod_lo, prod_hi)
    prev_prod = state["production_tph"]
    state["production_tph"] = prev_prod + prod_alpha * (desired_prod - prev_prod)

    # 3) O2 vs ID fan flow
    o2_nom = 2.6 + 0.000003 * (sp.get("id_fan_flow_Nm3_h", state["id_fan_flow_Nm3_h"]) - 150_000.0)
    o2_alpha = min(1.0, dt_sec / 5.0)
    prev_o2 = state["o2_percent"]
    state["o2_percent"] = clamp(prev_o2 + o2_alpha * (o2_nom - prev_o2), 2.0, 5.0)

    # Decide if we should update specific power this tick
    do_spower = True
    if SPOWER_MODE == "static":
        changed, cur_drivers = _drivers_changed_enough(state, sp)
        if not changed:
            do_spower = False
            _log_phys("SKIP_SP", mode=SPOWER_MODE, tol=SPOWER_TOL)
        else:
            state["_spower_drivers"] = cur_drivers

    if do_spower:
        k_base = (
            12.2
            - 0.25 * (state["production_tph"] - 10.0)
            + 0.001 * (sp.get("separator_dp_pa", state["separator_dp_pa"]) - 620.0)
            + 0.15  * (state["o2_percent"] - 2.6)
        )
        k_alpha = min(1.0, dt_sec / 10.0)
        prev_sp = state["specific_power_kwh_per_ton"]
        state["specific_power_kwh_per_ton"] = round(prev_sp + k_alpha * (k_base - prev_sp), 3)
        _log_phys("APPLY_SP", mode=SPOWER_MODE, prev=f"{prev_sp:.3f}", new=f"{state['specific_power_kwh_per_ton']:.3f}")

    # ---- PM sensor dynamics (NEW) ----
    sep = float(sp.get("separator_dp_pa", state["separator_dp_pa"]))
    o2  = float(state["o2_percent"])
    prod = float(state["production_tph"])

    rng = 0.02
    def walk(cur, drift, lo, hi):
        nxt = cur + drift + np.random.uniform(-rng, rng)
        return float(clamp(nxt, lo, hi))

    vib_drift = 0.0005*((sep-600)/100.0) + 0.002*max(0.0, 2.6 - o2)
    state["vibration_axial_mm_s"] = walk(state["vibration_axial_mm_s"], vib_drift, 0.5, 7.0)
    state["vibration_radial_mm_s"] = walk(state["vibration_radial_mm_s"], vib_drift*0.8, 0.5, 7.0)

    base_i = 130.0 + 1.6*prod
    ub = 3.0
    state["motor_current_a"] = walk(state["motor_current_a"], (base_i - state["motor_current_a"]) * 0.1, 80, 260)
    state["motor_current_b"] = walk(state["motor_current_b"], (base_i + 0.6*ub - state["motor_current_b"]) * 0.1, 80, 260)
    state["motor_current_c"] = walk(state["motor_current_c"], (base_i - 0.6*ub - state["motor_current_c"]) * 0.1, 80, 260)

    tgt_suction = 33.0 + 0.05*(prod-10.0)
    state["suction_temp_C"] = walk(state["suction_temp_C"], (tgt_suction - state["suction_temp_C"])*0.1, 20, 60)

    rise = 10.0 + 2.5*(state["vibration_axial_mm_s"] - 2.0)
    tgt_brg = state["suction_temp_C"] + clamp(rise, 5, 35)
    state["bearing_temp_C"] = walk(state["bearing_temp_C"], (tgt_brg - state["bearing_temp_C"])*0.08, 30, 120)

def _physics_step(state: Dict[str, float]) -> None:
    _physics_tick(state, dt_sec=MOCK_TICK_SEC)

def _mock_loop():
    last = time.monotonic()
    while True:
        time.sleep(MOCK_TICK_SEC)
        now = time.monotonic()
        dt = max(0.001, now - last)
        last = now
        with _state_lock:
            _physics_tick(_STATE, dt)
            snap = {k: v for k, v in _STATE.items() if k != "sp"}
            _append_history(snap)

if USE_MOCK:
    threading.Thread(target=_mock_loop, daemon=True).start()

# -------------------------
# Pydantic request models
# -------------------------
class RoutineOptimizeReq(BaseModel):
    snapshot: Optional[Dict[str, Any]] = None
    targets: Optional[Dict[str, Any]] = None
    constraints: Optional[Dict[str, Any]] = None
    apply_top: Optional[bool] = None
    log_suggestions: Optional[bool] = None
    nudge_if_neutral: Optional[bool] = None
    trigger: Optional[str] = Field(default=None, description="'cron'|'manual'")

class LoadOptimizeReq(BaseModel):
    snapshot: Optional[Dict[str, Any]] = None
    direction: Optional[str] = Field(None, pattern="^(up|down)$")
    delta_pct: Optional[float] = Field(None, gt=0, le=50)
    delta_abs: Optional[float] = None
    target_tph: Optional[float] = None
    steps: Optional[int] = None
    constraints: Optional[Dict[str, Any]] = None

class ApplyStageReq(BaseModel):
    current: Optional[Dict[str, Any]] = None
    setpoints: Optional[Dict[str, float]] = None
    stage: Optional[Dict[str, Any]] = None
    proposal: Optional[Dict[str, float]] = None
    proposed_setpoints: Optional[Dict[str, float]] = None

    plan_id: Optional[str] = None
    mode: Optional[str] = None
    stage_index: Optional[int] = None

    def extract_setpoints(self) -> Dict[str, float]:
        if isinstance(self.setpoints, dict):
            return {k: float(v) for k, v in self.setpoints.items()}
        if isinstance(self.stage, dict) and isinstance(self.stage.get("setpoints"), dict):
            return {k: float(v) for k, v in self.stage["setpoints"].items()}
        if isinstance(self.proposal, dict):
            return {k: float(v) for k, v in self.proposal.items()}
        if isinstance(self.proposed_setpoints, dict):
            return {k: float(v) for k, v in self.proposed_setpoints.items()}
        return {}

class SnapshotSetReq(BaseModel):
    setpoints: Dict[str, float]

# -------------------------
# BigQuery client (optional at runtime)
# -------------------------
_BQ_ENABLED = False
_BQ_ERR: Optional[str] = None
try:
    from google.cloud import bigquery  # type: ignore
    _bq_client = bigquery.Client()
    _BQ_ENABLED = True
except Exception as e:
    _bq_client = None
    _BQ_ENABLED = False
    _BQ_ERR = f"BigQuery client not initialized: {e}"

# ---------- Effective project & table FQNs ----------
def _effective_project() -> str:
    if PROJECT_ID:
        return PROJECT_ID
    if _bq_client and getattr(_bq_client, "project", None):
        return _bq_client.project  # type: ignore[attr-defined]
    raise HTTPException(status_code=500, detail="PROJECT_ID not found for BigQuery")

def _view_dataset() -> str:
    return VIEW_DATASET or "plant_ops"

def _pm_trends_view() -> str:
    return f"{_effective_project()}.{_view_dataset()}.pm_trends_last2h"

def _pm_segments_view() -> str:
    return f"{_effective_project()}.{_view_dataset()}.pm_segments_last3h"

def _pm_forecast_view() -> str:
    return f"{_effective_project()}.{_view_dataset()}.pm_forecast_next60m"

def _snapshots_table() -> str:
    return BQ_SNAPSHOTS_TABLE_ENV or f"{_effective_project()}.plant_ops.snapshots"

def _snapshots_latest_view() -> str:
    return BQ_SNAPSHOTS_VIEW_ENV or f"{_effective_project()}.plant_ops.ui_snapshot_latest"

def _plans_table() -> str:
    return BQ_PLANS_TABLE_ENV or f"{_effective_project()}.plant_ops.plans_v2"

def _acts_table() -> str:
    return BQ_ACTS_TABLE_ENV or f"{_effective_project()}.plant_ops.actuations_v2"

def _routine_table() -> str:
    return BQ_ROUTINE_TABLE_ENV or f"{_effective_project()}.plant_ops.routine_suggestions_v2"

def _suggestions_table() -> str:
    return BQ_SUGGESTIONS_TABLE_ENV or f"{_effective_project()}.plant_ops.suggestions_v1"

def _kpi_pred_latest_table() -> str:
    return BQ_KPI_PRED_LATEST_ENV or f"{_effective_project()}.plant_ops.kpi_predictions_latest"

def _kpi_pred_future_table() -> str:
    return BQ_KPI_PRED_FUTURE_ENV or f"{_effective_project()}.plant_ops.kpi_predictions_future"

def _bq_model_fqn() -> str:
    return f"{_effective_project()}.plant_ops.{MODEL_NAME}"

def _cm_table() -> Optional[str]:
    try:
        return BQ_CM_KPI_TABLE_ENV or f"{_effective_project()}.plant_ops.cm_kpis"
    except Exception:
        return None

# ---------- Small BQ helper ----------
def _bq_query(sql: str, params: Optional[List[Any]] = None):
    if not _BQ_ENABLED or _bq_client is None:
        raise HTTPException(status_code=503, detail=_BQ_ERR or "BigQuery unavailable")
    from google.cloud import bigquery  # type: ignore
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    job = _bq_client.query(sql, location=BQ_LOCATION, job_config=job_config)  # type: ignore
    return list(job.result())

def _latest_snapshot_from_bq() -> Dict[str, Any]:
    if not _BQ_ENABLED or _bq_client is None:
        return {}
    table = _snapshots_latest_view()
    sql = f"""
      SELECT
        production_tph, kiln_feed_tph, separator_dp_pa,
        id_fan_flow_Nm3_h, cooler_airflow_Nm3_h,
        kiln_speed_rpm, o2_percent,
        specific_power_kwh_per_ton, ts
      FROM `{table}`
      ORDER BY ts DESC
      LIMIT 1
    """
    rows = list(_bq_client.query(sql, location=BQ_LOCATION).result())
    if not rows:
        return {}
    r = dict(rows[0])
    if "ts" in r and isinstance(r["ts"], datetime.datetime):
        r["ts"] = r["ts"].isoformat()
    return r

# ---------- Flexible BQ helpers & recent-attempt memory ----------
_BQ_RECENT: List[Dict[str, Any]] = []

def _bq_get_schema(table_fqn: str) -> Dict[str, str]:
    try:
        tbl = _bq_client.get_table(table_fqn)  # type: ignore
        return {f.name: f.field_type for f in tbl.schema}
    except Exception:
        return {}

def _coerce_for_field(value: Any, field_type: str):
    if value is None:
        return None
    try:
        if field_type == "TIMESTAMP":
            if isinstance(value, datetime.datetime):
                return value.isoformat()
            return str(value)
        if field_type == "INT64":
            return int(value)
        if field_type in ("FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
            return float(value)
        if field_type == "BOOL":
            return bool(value)
        if field_type == "JSON":
            if isinstance(value, (dict, list)):
                return json.dumps(value, separators=(",", ":"))
            if isinstance(value, (int, float, bool)) or value is None:
                return json.dumps(value)
            return str(value)
        if field_type == "RECORD":
            if isinstance(value, (dict, list)):
                return value
            try:
                parsed = json.loads(value)
                return parsed
            except Exception:
                return {"value": value}
        if field_type == "STRING":
            return json.dumps(value, default=str) if isinstance(value, (dict, list)) else str(value)
        return value
    except Exception:
        return None

def _normalize_json_for_field(value: Any, field_type: Optional[str]) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if field_type == "RECORD" and not isinstance(parsed, dict):
                raise ValueError("RECORD field requires an object")
            return parsed
        except Exception:
            if field_type == "JSON":
                return {"value": value}
            raise
    if field_type == "JSON":
        return value
    if field_type == "RECORD":
        parsed = json.loads(json.dumps(value, default=str))
        if not isinstance(parsed, dict):
            raise ValueError("RECORD field requires an object (dict)")
        return parsed
    return value

def _bq_insert_flexible(table_fqn: Optional[str], payload: Dict[str, Any]) -> Optional[str]:
    if not _BQ_ENABLED or _bq_client is None or not table_fqn:
        return "BQ disabled or table not configured"
    schema = _bq_get_schema(table_fqn)
    if not schema:
        return f"Schema not found for {table_fqn}"
    row: Dict[str, Any] = {}
    for k, v in payload.items():
        if k in schema:
            row[k] = _coerce_for_field(v, schema[k])
    try:
        errors = _bq_client.insert_rows_json(table_fqn, [row])  # type: ignore
        if errors:
            return json.dumps(errors)
        return None
    except Exception as e:
        return str(e)

def _remember_bq_attempt(op: str, table: Optional[str], payload_keys: List[str], err: Optional[str]):
    _BQ_RECENT.append({
        "ts": _now_ts().isoformat(),
        "op": op,
        "table": table,
        "payload_keys": payload_keys,
        "error": err,
        "ok": err is None,
    })
    if len(_BQ_RECENT) > 50:
        del _BQ_RECENT[:-50]

def _bq_ingest_snapshot(snapshot: Dict[str, Any], source: str = "apply") -> Optional[str]:
    if not _BQ_ENABLED or _bq_client is None:
        return None
    table = _snapshots_table()
    schema = _bq_get_schema(table)
    row: Dict[str, Any] = {
        "ts": _now_ts().isoformat(),
        "source": source,
        "production_tph": float(snapshot["production_tph"]),
        "kiln_feed_tph": float(snapshot["kiln_feed_tph"]),
        "separator_dp_pa": float(snapshot["separator_dp_pa"]),
        "id_fan_flow_Nm3_h": float(snapshot["id_fan_flow_Nm3_h"]),
        "cooler_airflow_Nm3_h": float(snapshot["cooler_airflow_Nm3_h"]),
        "kiln_speed_rpm": float(snapshot["kiln_speed_rpm"]),
        "o2_percent": float(snapshot["o2_percent"]),
        "specific_power_kwh_per_ton": float(snapshot["specific_power_kwh_per_ton"]),
    }
    if "raw" in schema:
        row["raw"] = _as_json_string(snapshot)
    err = _bq_insert_flexible(table, row)
    _remember_bq_attempt("snapshot_insert", table, list(row.keys()), err)
    return err

def _cm_table_fq(backticked: bool = False) -> str:
    """
    Return the fully-qualified cm_kpis table as 'project.dataset.table'.
    If backticked=True, wrap it in BigQuery backticks.
    Uses _cm_table() if present, else env CM_KPIS_TABLE.
    """
    tbl = None
    try:
        tbl = _cm_table()
    except Exception:
        pass
    if not tbl:
        tbl = os.getenv("CM_KPIS_TABLE")
    if not tbl:
        raise RuntimeError("CM KPIs table not configured; set CM_KPIS_TABLE or implement _cm_table()")

    tbl = tbl.strip().strip("`")
    return f"`{tbl}`" if backticked else tbl


# -------------------------
# NEW: insert per-lever suggestions helper
# -------------------------
def _insert_suggestions_rows(suggestion_id: str,
                             created_at: datetime.datetime,
                             current: Dict[str, Any],
                             proposed_setpoints: Dict[str, Any],
                             constraints: Optional[Dict[str, Any]],
                             prediction_after: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    tbl = _suggestions_table()
    err_any: Optional[str] = None
    count = 0
    for lever, proposed in (proposed_setpoints or {}).items():
        cur = current.get(lever)
        try:
            cur_f = float(cur) if cur is not None else None
        except Exception:
            cur_f = None
        try:
            prop_f = float(proposed) if proposed is not None else None
        except Exception:
            prop_f = None
        delta_abs = (prop_f - cur_f) if (cur_f is not None and prop_f is not None) else None
        delta_pct = ((prop_f - cur_f) / cur_f * 100.0) if (cur_f not in (None, 0) and prop_f is not None) else None

        cmin = cmax = None
        if isinstance(constraints, dict):
            cmeta = constraints.get(lever, {})
            if isinstance(cmeta, dict):
                cmin = cmeta.get("min")
                cmax = cmeta.get("max")

        suggestion_text = _format_suggestion_text(lever, cur_f, prop_f, delta_pct, cmin, cmax)

        row = {
            "suggestion_row_id": str(uuid.uuid4()),
            "suggestion_id": suggestion_id,
            "created_at": created_at,
            "source": "routine",
            "lever": lever,
            "current_value": cur_f,
            "proposed_value": prop_f,
            "delta_abs": delta_abs,
            "delta_pct": delta_pct,
            "constraint_min": cmin,
            "constraint_max": cmax,
            "confidence": None,
            "suggestion_text": suggestion_text,
            "proposed_setpoints": proposed_setpoints,
            "snapshot_before": current,
            "prediction_after": prediction_after or None,
        }
        if _BQ_ENABLED and tbl:
            err = _bq_insert_flexible(tbl, row)
            _remember_bq_attempt("suggestion_insert", tbl, list(row.keys()), err)
            if err:
                logging.warning("suggestions_v1 insert error: %s", err)
                err_any = err
        count += 1
    return {"table": tbl, "insert_error": err_any, "rows_inserted": count}

# -------------------------
# Internal helpers: apply setpoints (reused by routes)
# -------------------------
_ACTS_RECENT: deque = deque(maxlen=50)

def _strip_debug_keys(m: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(m, dict):
        m.pop("_spower_drivers", None)
    return m

def _is_manual_actuation_request(request: Request) -> bool:
    """
    DEV MODE: Only requires 'X-Confirm-Apply: yes' header.
    (No Bearer token required.)
    """
    return (request.headers.get("X-Confirm-Apply", "") or request.headers.get("x-confirm-apply", "")).lower() == "yes"

def _apply_setpoints_internal(setpts: Dict[str, float],
                              mode: Optional[str] = None,
                              plan_id: Optional[str] = None,
                              stage_index: Optional[int] = None,
                              stage_name: Optional[str] = None) -> Dict[str, Any]:
    if not APPLY_ENABLED:
        res = {"ok": True, "note": "APPLY_ENABLED=0; dry-run"}
        _ACTS_RECENT.append(res)
        return res

    if not isinstance(setpts, dict) or not setpts:
        raise HTTPException(status_code=422, detail="No setpoints provided")

    nudge = float(os.getenv("MOCK_APPLY_NUDGE", "1.0"))
    settle_ticks = int(os.getenv("MOCK_APPLY_SETTLE_TICKS", "5"))

    if USE_MOCK:
        with _state_lock:
            before = {k: v for k, v in _STATE.items() if k != "sp"}
    else:
        before = {}

    if USE_MOCK:
        cfg = get_config()
        levers: Dict[str, Any] = cfg.get("levers", {})
        with _state_lock:
            sp = _STATE.setdefault("sp", {})
            for k, v in setpts.items():
                if k not in ("kiln_feed_tph","separator_dp_pa","id_fan_flow_Nm3_h","cooler_airflow_Nm3_h","kiln_speed_rpm"):
                    continue
                lo = levers.get(k, {}).get("min", -1e12)
                hi = levers.get(k, {}).get("max", 1e12)
                sp[k] = clamp(float(v), lo, hi)

            for k in ("kiln_feed_tph","separator_dp_pa","id_fan_flow_Nm3_h","cooler_airflow_Nm3_h","kiln_speed_rpm"):
                if k in sp:
                    cur = float(_STATE[k])
                    tgt = float(sp[k])
                    _STATE[k] = cur + nudge * (tgt - cur)

            for _ in range(max(0, settle_ticks)):
                _physics_tick(_STATE, dt_sec=MOCK_TICK_SEC)

            after = {k: v for k, v in _STATE.items() if k != "sp"}
            _append_history(after)
    else:
        after = {}

    before_clean = _strip_debug_keys(dict(before)) if isinstance(before, dict) else None
    after_clean  = _strip_debug_keys(dict(after)) if isinstance(after, dict) else None

    applied_at = _now_ts()

    if AUTO_INGEST_ON_APPLY and after_clean:
        try:
            _bq_ingest_snapshot(after_clean, source=f"apply:{mode or 'manual'}")
            global _LAST_INGEST_RUN
            _LAST_INGEST_RUN = applied_at
        except Exception as e:
            logging.warning("Auto-ingest after apply failed: %s", e)

    tbl = _acts_table()
    err = None
    if _BQ_ENABLED and tbl:
        schema = _bq_get_schema(tbl)

        def _maybe_stringify(val: Any, col: str):
            col_type = schema.get(col)
            if isinstance(val, (dict, list)) and col_type != "RECORD":
                return _as_json_string(val)
            return val

        deltas_obj = _diff_kpis(before_clean, after_clean) if before_clean and after_clean else None
        before_for_bq = _maybe_stringify(before_clean,  "before")
        after_for_bq  = _maybe_stringify(after_clean,   "after")
        deltas_for_bq = _maybe_stringify(deltas_obj, "deltas")

        act_row = {
            "applied_at": applied_at,
            "mode": mode,
            "plan_id": plan_id,
            "stage_index": stage_index,
            "stage_name": stage_name,
            "setpoints": setpts,
            "before": before_for_bq,
            "after": after_for_bq,
            "deltas": deltas_for_bq,
        }
        err = _bq_insert_flexible(tbl, act_row)
        _remember_bq_attempt("actuation_insert", tbl, list(act_row.keys()), err)
        if err:
            logging.warning("actuations_v2 insert error: %s", err)
    else:
        logging.info("Skipping actuation log: BQ_ENABLED=%s, table=%s", _BQ_ENABLED, tbl)

    def _round_map(m: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in m.items():
            if isinstance(v, (int, float)):
                out[k] = round(float(v), 3)
            else:
                out[k] = v
        return out

    res = {
        "ok": True,
        "applied_at": applied_at.isoformat(),
        "before": _round_map(before_clean) if isinstance(before_clean, dict) else None,
        "after": _round_map(after_clean) if isinstance(after_clean, dict) else None,
        "bq_log": {"table": tbl, "insert_error": err},
    }
    _ACTS_RECENT.append(res)
    return res

# -------------------------
# Last-run memory for scheduler & ingest
# -------------------------
_LAST_CRON_RUN: Optional[datetime.datetime] = None
_LAST_INGEST_RUN: Optional[datetime.datetime] = None

# -------------------------
# NEW: recent routine suggestions memory + helpers
# -------------------------
_ROUTINE_RECENT: deque = deque(maxlen=50)

def _maybe_parse_json(v: Any) -> Any:
    if isinstance(v, (dict, list)) or v is None:
        return v
    try:
        return json.loads(v)
    except Exception:
        return v

# -------------------------
# In-memory prediction stores (NEW)
# -------------------------
_PRED_LATEST_MEM: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
_PRED_FUTURE_MEM: Dict[str, List[Dict[str, Any]]] = {}

def _mem_pred_latest_write(kpi: str, horizon: int, points: List[Dict[str, Any]], pred_run_ts: datetime.datetime):
    _PRED_LATEST_MEM[(kpi, horizon)] = [
        dict(p, kpi_name=kpi, horizon_min=horizon, pred_run_ts=pred_run_ts.isoformat())
        for p in points
    ]

def _mem_pred_future_write(kpi: str, horizon: int, points: List[Dict[str, Any]], pred_run_ts: datetime.datetime):
    pred_ts = pred_run_ts.isoformat()
    _PRED_FUTURE_MEM[kpi] = [
        dict(p, kpi_name=kpi, horizon_min=horizon, pred_ts=pred_ts)
        for p in points
    ]

# -------------------------
# Routes
# -------------------------
@app.get("/")
def root():
    status, sheets = ensure_data_present()
    return {
        "service": "plant-agent",
        "version": SERVICE_VERSION,
        "mock": bool(USE_MOCK),
        "data_file": DATA_FILE,
        "data_status": status,
        "sheets": sheets,
        "health": "/healthz",
        "endpoints": [
            "/healthz", "/health", "/_ah/health", "/version", "/config",
            "/debug/config", "/debug/tables", "/debug/bq_recent", "/debug/last_runs",
            "/snapshot", "/snapshot/set",
            "/trends",
            "/pm/trends", "/pm/segments", "/pm/forecast",
            "/pm/health_index", "/pm/anomalies",
            "/optimize/routine", "/optimize/load", "/cron/routine",
            "/routine/latest",
            "/kpi/predictions/latest", "/kpi/predictions/future",
            "/actuate/apply_stage", "/actuate/rollback",
            "/actuations/latest", "/compare/latest",
            "/ingest", "/metrics",
            "/predict/spower",
            "/debug/physics_flags",
            # Condition monitoring endpoints
            "/cm/vhi", "/cm/vhi/log", "/cm/mci", "/cm/mci/log",
            # alias explicitly exposed
            "/segments/pm",
        ],
        "bq_enabled": _BQ_ENABLED,
        "sched_period_sec": SCHED_PERIOD_SEC,
    }

@app.get("/version")
def version():
    return {"version": SERVICE_VERSION, "sched_period_sec": SCHED_PERIOD_SEC}

@app.get("/healthz")
def healthz():
    try:
        _ = get_config()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return healthz()

@app.get("/_ah/health")
def gfe_health():
    return {"ok": True}

@app.head("/healthz")
def healthz_head():
    try:
        _ = get_config()
        return {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/config")
def config_get():
    return get_config()

@app.get("/debug/config")
def debug_config():
    known = ["/app/config/plant.yaml", "/app/plant.yaml", "config/plant.yaml", "plant.yaml"]
    try:
        effective_table = _snapshots_table()
    except Exception:
        effective_table = None
    try:
        proj_eff = _effective_project()
    except Exception:
        proj_eff = None
    return {
        "PLANT_CONFIG_env": os.getenv("PLANT_CONFIG"),
        "known_locations": known,
        "exists": {p: os.path.exists(p) for p in known},
        "resolved_path": _resolve_config_path() if os.getenv("PLANT_CONFIG") or any(os.path.exists(p) for p in known) else None,
        "keys": list(get_config().keys()),
        "bq_enabled": _BQ_ENABLED,
        "bq_error": _BQ_ERR,
        "bq_table_env": BQ_SNAPSHOTS_TABLE_ENV,
        "bq_table_effective": effective_table,
        "project_id_env": os.getenv("PROJECT_ID"),
        "project_id_effective": proj_eff,
        "sched_period_sec": SCHED_PERIOD_SEC,
        "spower_mode": SPOWER_MODE,
        "spower_tol": SPOWER_TOL,
        "log_physics": LOG_PHYSICS,
        "pm_views": {
            "trends": _pm_trends_view(),
            "segments": _pm_segments_view(),
            "forecast": _pm_forecast_view(),
        }
    }

@app.get("/debug/tables")
def debug_tables():
    try:
        proj = _effective_project()
    except Exception as e:
        proj = f"(error: {e})"
    return {
        "effective_project": proj,
        "snapshots_table_base": _snapshots_table(),
        "snapshots_latest_view": _snapshots_latest_view(),
        "plans_table": _plans_table(),
        "actuations_table": _acts_table(),
        "routine_table": _routine_table(),
        "suggestions_table": _suggestions_table(),
        # prediction tables
        "kpi_predictions_latest": _kpi_pred_latest_table(),
        "kpi_predictions_future": _kpi_pred_future_table(),
        # condition monitoring table
        "cm_kpis": _cm_table(),
        "pm_views": {
            "trends": _pm_trends_view(),
            "segments": _pm_segments_view(),
            "forecast": _pm_forecast_view(),
        },
        "bq_enabled": _BQ_ENABLED,
        "bq_location": BQ_LOCATION,
        "view_dataset": _view_dataset(),
    }

@app.get("/pm/health/latest")
def pm_health_latest():
    if not (_BQ_ENABLED and _bq_client):
        raise HTTPException(status_code=503, detail="BigQuery disabled")

    # Resolve CM table (backticked)
    tbl_name = _cm_table() or os.getenv("CM_KPIS_TABLE")
    if not tbl_name:
        raise HTTPException(status_code=500, detail="CM KPIs table not configured")
    tbl = f"`{tbl_name.strip().strip('`')}`"

    q = f"""
    WITH latest AS (
      SELECT
        kpi,
        ts,
        value,
        JSON_VALUE(aux, '$.status') AS status,
        ROW_NUMBER() OVER (PARTITION BY kpi ORDER BY ts DESC) AS rn
      FROM {tbl}
    )
    SELECT kpi, ts, value, status
    FROM latest
    WHERE rn = 1
    """
    rows = _bq_query(q)
    return {r["kpi"]: {"ts": r["ts"], "value": r["value"], "status": r["status"]} for r in rows}

@app.get("/debug/bq_recent")
def debug_bq_recent():
    return {
        "recent": list(reversed(_BQ_RECENT))[:10],
        "schemas": {
            "snapshots (base)": _bq_get_schema(_snapshots_table()) if _BQ_ENABLED else {},
            "plans_v2": _bq_get_schema(_plans_table()) if _BQ_ENABLED else {},
            "actuations_v2": _bq_get_schema(_acts_table()) if _BQ_ENABLED else {},
            "routine_suggestions_v2": _bq_get_schema(_routine_table()) if _BQ_ENABLED else {},
            "suggestions_v1": _bq_get_schema(_suggestions_table()) if _BQ_ENABLED else {},
            "kpi_predictions_latest": _bq_get_schema(_kpi_pred_latest_table()) if _BQ_ENABLED else {},
            "kpi_predictions_future": _bq_get_schema(_kpi_pred_future_table()) if _BQ_ENABLED else {},
            "cm_kpis": _bq_get_schema(_cm_table()) if (_BQ_ENABLED and _cm_table()) else {},
        },
        "tables": {
            "snapshots_base": _snapshots_table(),
            "snapshots_latest_view": _snapshots_latest_view(),
            "plans": _plans_table(),
            "acts": _acts_table(),
            "routine": _routine_table(),
            "suggestions": _suggestions_table(),
            "kpi_predictions_latest": _kpi_pred_latest_table(),
            "kpi_predictions_future": _kpi_pred_future_table(),
            "cm_kpis": _cm_table(),
            "pm_trends_view": _pm_trends_view(),
            "pm_segments_view": _pm_segments_view(),
            "pm_forecast_view": _pm_forecast_view(),
        },
        "enabled": _BQ_ENABLED,
        "bq_error": _BQ_ERR,
    }

# ---------- Countdown aliases ----------
@app.api_route("/debug/last_runs", methods=["GET","POST","OPTIONS"])
@app.api_route("/last_runs", methods=["GET","POST","OPTIONS"])
@app.api_route("/snapshot/last_runs", methods=["GET","POST","OPTIONS"])
@app.api_route("/debug/schedule", methods=["GET","POST","OPTIONS"])
def debug_last_runs(request: Request):
    now = _now_ts()

    last_cron = _LAST_CRON_RUN
    last_ing  = _LAST_INGEST_RUN

    if (last_cron is None or last_ing is None) and _BQ_ENABLED and (_bq_client is not None):
        try:
            if last_cron is None:
                sql_cron = f"SELECT MAX(created_at) AS ts FROM `{_routine_table()}`"
                rows_cron = list(_bq_client.query(sql_cron, location=BQ_LOCATION).result())
                ts = rows_cron[0].ts if rows_cron and getattr(rows_cron[0], "ts", None) else None
                if ts:
                    last_cron = ts if isinstance(ts, datetime.datetime) else datetime.datetime.fromisoformat(str(ts))
                    if last_cron.tzinfo is None:
                        last_cron = last_cron.replace(tzinfo=datetime.timezone.utc)
            if last_ing is None:
                sql_ing = f"SELECT MAX(ts) AS ts FROM `{_snapshots_table()}`"
                rows_ing = list(_bq_client.query(sql_ing, location=BQ_LOCATION).result())
                ts = rows_ing[0].ts if rows_ing and getattr(rows_ing[0], "ts", None) else None
                if ts:
                    last_ing = ts if isinstance(ts, datetime.datetime) else datetime.datetime.fromisoformat(str(ts))
                    if last_ing.tzinfo is None:
                        last_ing = last_ing.replace(tzinfo=datetime.timezone.utc)
        except Exception as e:
            logging.info("BQ fallback disabled/unavailable: %s", e)

    next_eta = None
    sec_to_next = None
    if last_cron is not None:
        next_eta = last_cron + datetime.timedelta(seconds=SCHED_PERIOD_SEC)
        sec_to_next = max(0, int((next_eta - now).total_seconds()))

    out = {
        "last_cron_routine": last_cron.isoformat() if last_cron else None,
        "last_ingest": last_ing.isoformat() if last_ing else None,
        "sched_period_sec": SCHED_PERIOD_SEC,
        "now": now.isoformat(),
        "next_cron_eta": next_eta.isoformat() if next_eta else None,
        "seconds_to_next": sec_to_next,
    }
    logging.info("last_runs seconds_to_next=%s next=%s", out["seconds_to_next"], out["next_cron_eta"])
    return out

@app.api_route("/debug/last_runs/", methods=["GET","POST","OPTIONS"])
@app.api_route("/last_runs/", methods=["GET","POST","OPTIONS"])
@app.api_route("/snapshot/last_runs/", methods=["GET","POST","OPTIONS"])
@app.api_route("/debug/schedule/", methods=["GET","POST","OPTIONS"])
def debug_last_runs_slash(request: Request):
    return debug_last_runs(request)

@app.get("/snapshot")
def snapshot(source: Optional[str] = Query(default="auto", description="'auto'|'mock'|'bq'")):
    if source == "bq":
        s = _latest_snapshot_from_bq()
        if not s:
            raise HTTPException(status_code=404, detail="No snapshot in BigQuery")
        return s

    if source == "mock" or (source == "auto" and USE_MOCK):
        with _state_lock:
            return {k: v for k, v in _STATE.items() if k != "sp"}

    s = _latest_snapshot_from_bq()
    if not s:
        raise HTTPException(status_code=501, detail="Live plant connectors not configured")
    return s

@app.post("/snapshot/set")
def snapshot_set(req: SnapshotSetReq):
    if not USE_MOCK:
        raise HTTPException(status_code=405, detail="Not allowed on live plant")
    with _state_lock:
        for k, v in req.setpoints.items():
            if k in _STATE and isinstance(v, (int, float)):
                _STATE[k] = float(v)
        _physics_step(_STATE)
        snap = {k: v for k, v in _STATE.items() if k != "sp"}
        _append_history(snap)
    return {"ok": True, "state": snap}

# -------------------------
# /trends (historical plant KPIs from snapshots)
# -------------------------
def _sanitize_trend_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _f(v):
        try:
            if isinstance(v, str):
                v = v.replace(",", "")
            return float(v)
        except Exception:
            return None

    rows_sorted = sorted(rows, key=lambda r: r.get("ts") or "")
    out: List[Dict[str, Any]] = []
    last_p = None
    last_sp = None

    for r in rows_sorted:
        p = _f(r.get("production_tph"))
        o2 = _f(r.get("o2_percent"))
        sp = _f(r.get("specific_power_kwh_per_ton"))

        ok_p = p is not None and 0.5 <= p <= 1000
        ok_o2 = o2 is not None and 0.0 <= o2 <= 30.0
        ok_sp = sp is not None and 0.1 <= sp <= 300.0

        if ok_p and last_p is not None:
            if abs(p - last_p) / max(1e-6, last_p) > 0.30:
                p = None
                ok_p = False
        if ok_sp and last_sp is not None:
            if abs(sp - last_sp) / max(1e-6, last_sp) > 0.30:
                sp = None
                ok_sp = False

        if ok_p:
            last_p = p
        if ok_sp:
            last_sp = sp

        out.append({
            "ts": r.get("ts"),
            "production_tph": p if ok_p else None,
            "o2_percent": o2 if ok_o2 else None,
            "specific_power_kwh_per_ton": sp if ok_sp else None,
        })
    return out

@app.get("/trends")
def trends(
    minutes: int = Query(default=60, ge=1, le=24*60),
    limit: int = Query(default=180, ge=1, le=5000),
    source: str = Query(default="auto", description="'auto'|'mock'|'bq'"),
    clean: int = Query(default=1, ge=0, le=1),
):
    if source == "mock" or (source == "auto" and USE_MOCK):
        cutoff = _now_ts() - datetime.timedelta(minutes=minutes)
        pts = [p for p in list(_HIST) if datetime.datetime.fromisoformat(p["ts"].replace("Z","")).astimezone(datetime.timezone.utc) >= cutoff]
        pts = pts[-limit:]
        out = _sanitize_trend_rows(pts) if clean else pts
        try:
            with _state_lock:
                snap = {k: v for k, v in _STATE.items() if k != "sp"}
            last = out[-1] if out else {}
            logging.info(
                "trends(mock) count=%d first=%s last=%s snap_prod=%.3f last_prod=%s",
                len(out), out[0]["ts"] if out else None, last.get("ts"),
                float(snap.get("production_tph", 0.0)),
                last.get("production_tph"),
            )
        except Exception:
            pass
        return out

    if not _BQ_ENABLED or _bq_client is None:
        raise HTTPException(status_code=503, detail="BigQuery disabled")

    from google.cloud import bigquery  # type: ignore
    table = _snapshots_table()
    sql = f"""
      SELECT
        ts,
        production_tph,
        o2_percent,
        specific_power_kwh_per_ton
      FROM `{table}`
      WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @m MINUTE)
      ORDER BY ts ASC
      LIMIT @lim
    """
    job = _bq_client.query(
        sql,
        location=BQ_LOCATION,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("m", "INT64", minutes),
                bigquery.ScalarQueryParameter("lim", "INT64", limit),
            ]
        ),
    )
    rows = [{"ts": r.get("ts").isoformat() if r.get("ts") else None,
             "production_tph": r.get("production_tph"),
             "o2_percent": r.get("o2_percent"),
             "specific_power_kwh_per_ton": r.get("specific_power_kwh_per_ton")} for r in job.result()]
    out = _sanitize_trend_rows(rows) if clean else rows
    return out

# -------------------------
# NEW: PM View Readers (BigQuery views you created)
# -------------------------
from fastapi import status as _status

@app.api_route("/pm/trends", methods=["GET", "HEAD"])
def pm_trends_view():
    """
    Read from view: {project}.{VIEW_DATASET}.pm_trends_last2h
    Columns: ts, vhi_health_index, mci_percent, bearing_temp_rise_C
    """
    if not _BQ_ENABLED or _bq_client is None:
        raise HTTPException(status_code=503, detail=_BQ_ERR or "BigQuery unavailable")
    sql = f"""
      SELECT ts, vhi_health_index, mci_percent, bearing_temp_rise_C
      FROM `{_pm_trends_view()}`
      ORDER BY ts
    """
    rows = _bq_query(sql)
    out = []
    for r in rows:
        d = dict(r)
        ts = d.get("ts")
        d["ts"] = ts.isoformat() if isinstance(ts, datetime.datetime) else str(ts)
        out.append(d)
    return out

# Trailing-slash alias and explicit OPTIONS for /pm/trends
@app.api_route("/pm/trends/", methods=["GET", "HEAD"])
def pm_trends_view_slash():
    return pm_trends_view()

@app.options("/pm/trends")
def options_pm_trends(request: Request):
    origin = request.headers.get("origin")
    return Response(status_code=_status.HTTP_204_NO_CONTENT, headers=_cors_hdrs_for(origin) | {"Allow": "GET, HEAD, OPTIONS"})

@app.options("/pm/trends/")
def options_pm_trends_slash(request: Request):
    origin = request.headers.get("origin")
    return Response(status_code=_status.HTTP_204_NO_CONTENT, headers=_cors_hdrs_for(origin) | {"Allow": "GET, HEAD, OPTIONS"})

# >>>>>>>>>>>> HARDENED: GET+HEAD + alias + OPTIONS + minutes filter
@app.api_route("/pm/segments", methods=["GET", "HEAD"])
def pm_segments_view(minutes: Optional[int] = Query(default=None, ge=1, le=24*60)):
    """
    Read from view: {project}.{VIEW_DATASET}.pm_segments_last3h
    Columns: start_ts, end_ts, status ("ok"|"watch"|"alert"), optional kpi/note.
    If ?minutes is provided, filter to that recent window on start_ts.
    """
    if not _BQ_ENABLED or _bq_client is None:
        raise HTTPException(status_code=503, detail=_BQ_ERR or "BigQuery unavailable")

    if minutes is None:
        sql = f"""
          SELECT start_ts, end_ts, status, kpi, note
          FROM `{_pm_segments_view()}`
          ORDER BY start_ts
        """
        rows = _bq_query(sql)
    else:
        from google.cloud import bigquery  # type: ignore
        sql = f"""
          SELECT start_ts, end_ts, status, kpi, note
          FROM `{_pm_segments_view()}`
          WHERE start_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @m MINUTE)
          ORDER BY start_ts
        """
        rows = _bq_query(sql, params=[bigquery.ScalarQueryParameter("m", "INT64", minutes)])

    out = []
    for r in rows:
        d = dict(r)
        st = d.get("start_ts")
        et = d.get("end_ts")
        d["start_ts"] = st.isoformat() if isinstance(st, datetime.datetime) else str(st)
        d["end_ts"]   = et.isoformat() if isinstance(et, datetime.datetime) else (str(et) if et else None)
        out.append({k: v for k, v in d.items() if k in ("start_ts","end_ts","status","kpi","note")})
    return out

# Friendly alias because your UI sometimes calls /segments/pm
@app.api_route("/segments/pm", methods=["GET", "HEAD"])
def pm_segments_view_alias(minutes: Optional[int] = Query(default=None, ge=1, le=24*60)):
    return pm_segments_view(minutes=minutes)

# Explicit OPTIONS (some proxies/hosts are picky)
@app.options("/pm/segments")
def options_pm_segments(request: Request):
    origin = request.headers.get("origin")
    return Response(status_code=_status.HTTP_204_NO_CONTENT, headers=_cors_hdrs_for(origin) | {"Allow": "GET, HEAD, OPTIONS"})

@app.options("/segments/pm")
def options_segments_pm(request: Request):
    origin = request.headers.get("origin")
    return Response(status_code=_status.HTTP_204_NO_CONTENT, headers=_cors_hdrs_for(origin) | {"Allow": "GET, HEAD, OPTIONS"})

@app.api_route("/pm/forecast", methods=["GET", "HEAD"])
def pm_forecast_view():
    """
    Optional: reads {project}.{VIEW_DATASET}.pm_forecast_next60m if present.
    Expected columns: ts, vhi_pred, mci_pred, btr_pred
    """
    if not _BQ_ENABLED or _bq_client is None:
        raise HTTPException(status_code=503, detail=_BQ_ERR or "BigQuery unavailable")
    view_fqn = _pm_forecast_view()
    try:
        sql = f"SELECT ts, vhi_pred, mci_pred, btr_pred FROM `{view_fqn}` ORDER BY ts"
        rows = _bq_query(sql)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Forecast view not found or query failed: {view_fqn}. {e}")
    out = []
    for r in rows:
        d = dict(r)
        ts = d.get("ts")
        d["ts"] = ts.isoformat() if isinstance(ts, datetime.datetime) else str(ts)
        out.append(d)
    return out

@app.options("/pm/forecast")
def options_pm_forecast(request: Request):
    origin = request.headers.get("origin")
    return Response(status_code=_status.HTTP_204_NO_CONTENT, headers=_cors_hdrs_for(origin) | {"Allow": "GET, HEAD, OPTIONS"})

# -------------------------
# Internal: threshold filter & optional neutral nudge
# -------------------------
def _build_per_lever_audit(s: Dict[str, Any],
                           naive: Dict[str, Any],
                           levers_meta: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in naive.items():
        cur = s.get(k)
        try:
            cur_f = float(cur) if cur is not None else None
        except Exception:
            cur_f = None
        try:
            prop_f = float(v) if v is not None else None
        except Exception:
            prop_f = None

        if cur_f is not None and prop_f is not None:
            d_abs = prop_f - cur_f
            d_pct = (prop_f / cur_f - 1.0) if cur_f != 0 else None
        else:
            d_abs, d_pct = None, None

        out[k] = {
            "current": cur_f,
            "proposed_before_filter": prop_f,
            "delta_abs": d_abs,
            "delta_pct": d_pct,
            "bounds": {
                "min": levers_meta.get(k, {}).get("min"),
                "max": levers_meta.get(k, {}).get("max"),
            },
            "decision": "pending",
            "rule": None,
        }
    return out

def _apply_threshold_filters(per: Dict[str, Any]) -> Tuple[Dict[str, float], Dict[str, Any]]:
    filtered: Dict[str, float] = {}
    for k, info in per.items():
        cur = info["current"]
        prop = info["proposed_before_filter"]
        d_abs = info["delta_abs"]
        d_pct = info["delta_pct"]
        lo = info["bounds"]["min"]
        hi = info["bounds"]["max"]

        if cur is None or prop is None:
            info["decision"] = "skip_missing_neutral"
            info["rule"] = "missing_current_or_proposed"
            continue

        if MIN_PCT_DELTA > 0 and cur != 0 and d_pct is not None and abs(d_pct) < MIN_PCT_DELTA:
            info["decision"] = "skip_small"
            info["rule"] = f"abs(delta_pct)<{MIN_PCT_DELTA}"
            continue

        if k == "id_fan_flow_Nm3_h" and MIN_ABS_ID_FAN > 0 and abs(d_abs) < MIN_ABS_ID_FAN:
            info["decision"] = "skip_small"
            info["rule"] = f"abs(delta_abs)<{MIN_ABS_ID_FAN}"
            continue

        if k == "cooler_airflow_Nm3_h" and MIN_ABS_COOLER > 0 and abs(d_abs) < MIN_ABS_COOLER:
            info["decision"] = "skip_small"
            info["rule"] = f"abs(delta_abs)<{MIN_ABS_COOLER}"
            continue

        proposed = prop
        if lo is not None:
            proposed = max(float(lo), proposed)
        if hi is not None:
            proposed = min(float(hi), proposed)

        info["proposed"] = round(float(proposed), 3)
        info["decision"] = "keep"
        info["rule"] = "passed_thresholds"
        filtered[k] = info["proposed"]
    return filtered, per

def _maybe_neutral_nudge(s: Dict[str, Any],
                         levers_meta: Dict[str, Any],
                         want_nudge: bool) -> Tuple[Dict[str, float], Dict[str, Any], Optional[str]]:
    if not want_nudge:
        return {}, {}, None

    out: Dict[str, float] = {}
    per: Dict[str, Any] = {}
    detail_bits = []
    for k, min_abs in (("id_fan_flow_Nm3_h", MIN_ABS_ID_FAN), ("cooler_airflow_Nm3_h", MIN_ABS_COOLER)):
        cur = s.get(k)
        if cur is None:
            continue
        try:
            cur_f = float(cur)
        except Exception:
            continue
        base_step = max(min_abs, 0.01 * abs(cur_f))
        tgt = cur_f - base_step
        lo = levers_meta.get(k, {}).get("min", -1e12)
        hi = levers_meta.get(k, {}).get("max", 1e12)
        tgt = clamp(tgt, lo, hi)
        out[k] = round(tgt, 3)
        d_abs = tgt - cur_f
        d_pct = (tgt / cur_f - 1.0) if cur_f != 0 else None
        per[k] = {
            "current": cur_f,
            "proposed_before_filter": tgt,
            "proposed": tgt,
            "delta_abs": d_abs,
            "delta_pct": d_pct,
            "bounds": {"min": lo, "max": hi},
            "decision": "nudge",
            "rule": "neutral_nudge",
        }
        detail_bits.append(f"{k}: {cur_f:.0f}→{tgt:.0f} (−{abs(d_abs):.0f})")
    reason_extra = ("Applied neutral nudge — " + ", ".join(detail_bits)) if detail_bits else None
    return out, per, reason_extra

def _normalize_routine_flags(req: RoutineOptimizeReq) -> Dict[str, bool]:
    apply_top = CRON_APPLY_TOP if req.apply_top is None else bool(req.apply_top)
    log_suggestions = CRON_LOG_SUGGESTIONS if req.log_suggestions is None else bool(req.log_suggestions)
    nudge_if_neutral = CRON_NUDGE_IF_NEUTRAL if req.nudge_if_neutral is None else bool(req.nudge_if_neutral)
    return {
        "apply_top": apply_top,
        "log_suggestions": log_suggestions,
        "nudge_if_neutral": nudge_if_neutral,
    }

@app.post("/optimize/routine")
def optimize_routine(req: RoutineOptimizeReq):
    flags = _normalize_routine_flags(req)

    if req.snapshot and isinstance(req.snapshot, dict):
        s = dict(req.snapshot)
        used_source = "request.snapshot"
        used_ts = s.get("ts")
    else:
        if USE_MOCK:
            with _state_lock:
                s = {k: v for k, v in _STATE.items() if k != "sp"}
            used_source = "mock"
            used_ts = _now_ts().isoformat()
        else:
            s = _latest_snapshot_from_bq()
            used_source = "bq"
            used_ts = s.get("ts")
        if not s:
            raise HTTPException(status_code=422, detail="No snapshot provided and none available")

    used_hash = _md5_short(s)

    cfg = get_config()
    levers: Dict[str, Any] = cfg.get("levers", {})

    recipe: Dict[str, float] = {}
    for lever, meta in levers.items():
        if meta.get("hold_in_routine"):
            continue
        if lever == "separator_dp_pa":
            recipe[lever] = 600.0
        elif lever == "id_fan_flow_Nm3_h":
            base = s.get("id_fan_flow_Nm3_h", 150000) * 0.98
            recipe[lever] = max(meta.get("min", 0), min(meta.get("max", 1e12), base))
        elif lever == "cooler_airflow_Nm3_h":
            base = s.get("cooler_airflow_Nm3_h", 220000) * 0.98
            recipe[lever] = max(meta.get("min", 0), min(meta.get("max", 1e12), base))

    proposal_list = propose_actions(s, recipe, levers)
    naive_proposal = proposal_list[0] if proposal_list else {}

    if not naive_proposal:
        fallback = {}
        if "id_fan_flow_Nm3_h" in s and "id_fan_flow_Nm3_h" in levers:
            lo, hi = levers["id_fan_flow_Nm3_h"].get("min", -1e12), levers["id_fan_flow_Nm3_h"].get("max", 1e12)
            fallback["id_fan_flow_Nm3_h"] = clamp(s["id_fan_flow_Nm3_h"] * 0.98, lo, hi)
        if "cooler_airflow_Nm3_h" in s and "cooler_airflow_Nm3_h" in levers:
            lo, hi = levers["cooler_airflow_Nm3_h"].get("min", -1e12), levers["cooler_airflow_Nm3_h"].get("max", 1e12)
            fallback["cooler_airflow_Nm3_h"] = clamp(s["cooler_airflow_Nm3_h"] * 0.98, lo, hi)
        if "separator_dp_pa" in s and "separator_dp_pa" in levers:
            lo, hi = levers["separator_dp_pa"].get("min", -1e12), levers["separator_dp_pa"].get("max", 1e12)
            fallback["separator_dp_pa"] = clamp(600.0, lo, hi)
        naive_proposal = {k: round(v, 3) for k, v in fallback.items()}

    per_lever = _build_per_lever_audit(s, naive_proposal, levers)
    filtered_proposal, per_lever = _apply_threshold_filters(per_lever)

    reason = None
    reason_detail = None

    if filtered_proposal:
        reason = "proposed"
        kept = [f"{k}→{v}" for k, v in filtered_proposal.items()]
        reason_detail = f"Changes passed thresholds (MIN_PCT_DELTA={MIN_PCT_DELTA}, MIN_ABS_ID_FAN={MIN_ABS_ID_FAN}, MIN_ABS_COOLER={MIN_ABS_COOLER}). Kept: {', '.join(kept)}"
    else:
        reason = "neutral"
        suppressed = [k for k, info in per_lever.items() if info.get("decision") in ("skip_small", "skip_missing_neutral")]
        reason_detail = f"All candidate changes below thresholds (MIN_PCT_DELTA={MIN_PCT_DELTA}, MIN_ABS_ID_FAN={MIN_ABS_ID_FAN}, MIN_ABS_COOLER={MIN_ABS_COOLER}). Suppressed: {', '.join(suppressed) if suppressed else '—'}"

        nudge_prop, nudge_per, nudge_detail = _maybe_neutral_nudge(s, levers, bool(flags["nudge_if_neutral"]))
        if nudge_prop:
            for k, info in nudge_per.items():
                per_lever[k] = info
            filtered_proposal = nudge_prop
            reason = "nudge_applied"
            if nudge_detail:
                reason_detail = (reason_detail + " | " + nudge_detail) if reason_detail else nudge_detail

    proposed_final = {k: round(float(v), 3) for k, v in filtered_proposal.items()} if filtered_proposal else {}

    pred = {"specific_power_kwh_per_ton": predict_specific_power(s)}
    payload = {
        "mode": "routine",
        "current": s,
        "predicted_after": pred,
        "actions": {"apply_stage": True, "apply_all": False, "rollback": True},
        "proposed_setpoints": proposed_final,
        "per_lever": per_lever,
        "reason": reason,
        "reason_detail": reason_detail,
        "match_info": {"candidates_used": 2008},
        "used_snapshot_source": used_source,
        "used_snapshot_ts": used_ts,
        "used_snapshot_hash": _md5_short(s),
        "flags_effective": flags,
        "constraints": req.constraints,
        "targets": req.targets,
    }

    tbl = _routine_table()
    err = None
    suggestion_id = str(uuid.uuid4())
    created_at = _now_ts()
    if _BQ_ENABLED and tbl:
        schema = _bq_get_schema(tbl)
        proposed_for_bq = proposed_final
        if schema.get("proposed_setpoints") == "JSON" and isinstance(proposed_for_bq, (dict, list)):
            proposed_for_bq = _as_json_string(proposed_for_bq)

        suggestion_row = {
            "suggestion_id": suggestion_id,
            "created_at": created_at,
            "snapshot": s,
            "proposed_setpoints": proposed_for_bq,
            "predicted_after": pred,
            "targets": req.targets,
            "constraints": req.constraints,
            "mode": "routine",
            "reason": reason,
            "reason_detail": reason_detail,
            "used_snapshot_source": used_source,
            "used_snapshot_ts": used_ts,
            "used_snapshot_hash": _md5_short(s),
        }
        err = _bq_insert_flexible(tbl, suggestion_row)
        _remember_bq_attempt("routine_insert", tbl, list(suggestion_row.keys()), err)
        if err:
            logging.warning("routine_suggestions_v2 insert error: %s", err)
    else:
        logging.info("Skipping routine suggestion logging: BQ_ENABLED=%s, table=%s", _BQ_ENABLED, tbl)

    sugg_log = None
    if flags["log_suggestions"] and proposed_final:
        try:
            sugg_log = _insert_suggestions_rows(suggestion_id, created_at, s, proposed_final, req.constraints or {}, pred)
        except Exception as e:
            logging.warning("suggestions_v1 insert exception: %s", e)
            sugg_log = {"table": _suggestions_table(), "insert_error": str(e), "rows_inserted": 0}

    actuation = None
    applied = False
    if (req.trigger or "").lower() != "cron":
        if flags["apply_top"] and isinstance(proposed_final, dict) and proposed_final:
            try:
                actuation = _apply_setpoints_internal(proposed_final, mode="routine", plan_id=None,
                                                      stage_index=None, stage_name="routine_auto_apply")
                applied = True and (actuation.get("bq_log", {}).get("insert_error") is None)
            except HTTPException as e:
                logging.warning("Auto-apply failed: %s", e.detail)
                actuation = {"ok": False, "error": e.detail}
            except Exception as e:
                logging.warning("Auto-apply exception: %s", e)
                actuation = {"ok": False, "error": str(e)}

    payload["bq_log"] = {"table": tbl, "insert_error": err}
    if sugg_log:
        payload["suggestions_log"] = sugg_log
    payload["applied"] = bool(applied)
    if actuation:
        payload["actuation"] = actuation
    payload["suggestion_id"] = suggestion_id
    payload["created_at"] = created_at.isoformat()

    try:
        _ROUTINE_RECENT.append({
            "suggestion_id": suggestion_id,
            "created_at": created_at.isoformat(),
            "proposed_setpoints": proposed_final,
            "applied": bool(applied),
            "actuation": actuation,
            "bq_log": payload.get("bq_log"),
            "suggestions_log": payload.get("suggestions_log"),
            "current": s,
            "predicted_after": pred,
            "mode": "routine",
            "per_lever": per_lever,
            "reason": reason,
            "reason_detail": reason_detail,
            "used_snapshot_source": used_source,
            "used_snapshot_ts": used_ts,
            "used_snapshot_hash": _md5_short(s),
            "flags_effective": flags,
        })
    except Exception:
        pass

    return payload

@app.post("/cron/routine")
def cron_routine(body: dict = Body(default={})):
    """Strictly read-only cron: never applies, stamps last_cron."""
    global _LAST_CRON_RUN
    _LAST_CRON_RUN = _now_ts()

    req = RoutineOptimizeReq(
        snapshot=body.get("snapshot"),
        targets=body.get("targets"),
        constraints=body.get("constraints"),
        apply_top=False,
        log_suggestions=body.get("log_suggestions", CRON_LOG_SUGGESTIONS),
        nudge_if_neutral=body.get("nudge_if_neutral", CRON_NUDGE_IF_NEUTRAL),
        trigger="cron",
    )
    return optimize_routine(req)

@app.get("/routine/latest")
def routine_latest():
    if _ROUTINE_RECENT:
        return _ROUTINE_RECENT[-1]

    if not _BQ_ENABLED or _bq_client is None:
        raise HTTPException(status_code=404, detail="No routine suggestions available")

    from google.cloud import bigquery  # type: ignore
    table = _routine_table()
    sql = f"""
      SELECT suggestion_id, created_at, proposed_setpoints, predicted_after, snapshot,
             reason, reason_detail, used_snapshot_source, used_snapshot_ts, used_snapshot_hash
      FROM `{table}`
      ORDER BY created_at DESC
      LIMIT 1
    """
    rows = list(_bq_client.query(sql, location=BQ_LOCATION).result())
    if not rows:
        raise HTTPException(status_code=404, detail="No routine suggestions in BigQuery")
    r = dict(rows[0])

    return {
        "suggestion_id": r.get("suggestion_id"),
        "created_at": r.get("created_at").isoformat() if r.get("created_at") else None,
        "proposed_setpoints": _maybe_parse_json(r.get("proposed_setpoints")),
        "predicted_after": _maybe_parse_json(r.get("predicted_after")),
        "current": _maybe_parse_json(r.get("snapshot")),
        "applied": False,
        "mode": "routine",
        "reason": r.get("reason") or "from_bq_history",
        "reason_detail": r.get("reason_detail") or "latest suggestion loaded from BigQuery history; per_lever audit only available for in-memory recent runs on this instance",
        "used_snapshot_source": r.get("used_snapshot_source"),
        "used_snapshot_ts": r.get("used_snapshot_ts"),
        "used_snapshot_hash": r.get("used_snapshot_hash"),
    }

# -------------------------
# Optimize (Load Up/Down) + Plan Logging
# -------------------------
@app.post("/optimize/load")
def optimize_load(req: LoadOptimizeReq):
    if req.snapshot and isinstance(req.snapshot, dict):
        s = dict(req.snapshot)
    else:
        if USE_MOCK:
            with _state_lock:
                s = {k: v for k, v in _STATE.items() if k != "sp"}
        else:
            s = _latest_snapshot_from_bq()
        if not s:
            raise HTTPException(status_code=422, detail="No snapshot provided and none available")

    cfg = get_config()
    levers: Dict[str, Any] = cfg.get("levers", {})
    cadence: Dict[str, Any] = cfg.get("cadence", {})
    stages_max = int(cadence.get("stages_max", 4))

    if "production_tph" not in s or s["production_tph"] <= 0:
        raise HTTPException(status_code=422, detail="snapshot.production_tph is required and > 0")

    base_prod = float(s["production_tph"])
    target: Optional[float] = None
    if req.target_tph is not None:
        target = float(req.target_tph)
    elif req.delta_abs is not None:
        target = base_prod + float(req.delta_abs)
    elif req.delta_pct is not None:
        pct = float(req.delta_pct)
        dir_factor = 1.0 if (req.direction or "up") == "up" else -1.0
        target = base_prod * (1.0 + dir_factor * pct / 100.0)
    else:
        raise HTTPException(status_code=422, detail="Provide one of target_tph, delta_abs, or delta_pct")

    direction = req.direction
    if direction is None:
        direction = "up" if target > base_prod else "down"

    delta_abs_calc = target - base_prod
    delta_pct_calc = (target / base_prod - 1.0) * 100.0 if base_prod else None

    targets: Dict[str, float] = {}
    if "kiln_feed_tph" in s and "kiln_feed_tph" in levers:
        targets["kiln_feed_tph"] = target
    if "separator_dp_pa" in s and "separator_dp_pa" in levers:
        targets["separator_dp_pa"] = 660.0 if direction == "up" else 600.0
    if "id_fan_flow_Nm3_h" in s and "id_fan_flow_Nm3_h" in levers:
        scale = 1.04 if direction == "up" else 0.97
        targets["id_fan_flow_Nm3_h"] = s["id_fan_flow_Nm3_h"] * scale
    if "cooler_airflow_Nm3_h" in s and "cooler_airflow_Nm3_h" in levers:
        scale = 1.08 if direction == "up" else 0.95
        targets["cooler_airflow_Nm3_h"] = s["cooler_airflow_Nm3_h"] * scale
    if "kiln_speed_rpm" in s and "kiln_speed_rpm" in levers:
        if direction == "up":
            targets["kiln_speed_rpm"] = min(levers["kiln_speed_rpm"].get("max", 4.2), s["kiln_speed_rpm"] + 0.072)
        else:
            targets["kiln_speed_rpm"] = s["kiln_speed_rpm"]

    stages = build_stage_plan(s, targets, levers, stages_max=req.steps or stages_max)

    def _has_moves(stages_list: List[Dict[str, Any]]) -> bool:
        for st in stages_list or []:
            spv = st.get("setpoints") or {}
            if any(abs(float(spv[k]) - float(s.get(k, spv[k]))) > 1e-6 for k in spv.keys()):
                return True
        return False

    if not stages or not _has_moves(stages):
        direct = {k: float(v) for k, v in targets.items()
                  if k in ("kiln_feed_tph","separator_dp_pa","id_fan_flow_Nm3_h","cooler_airflow_Nm3_h","kiln_speed_rpm")}
        if direct:
            stages = [{
                "name": "Direct Apply",
                "setpoints": direct,
                "checks": [
                    "O2 in [2.5, 4.5] %",
                    "CO < 180 ppm",
                    "Bagfilter ΔP < 1800 Pa",
                ],
            }]

    pred = {"specific_power_kwh_per_ton": predict_specific_power(s)}

    plan_id = str(uuid.uuid4())
    created_at = _now_ts()
    payload = {
        "plan_id": plan_id,
        "created_at": created_at.isoformat(),
        "mode": f"load_{direction}",
        "current": s,
        "predicted_after": pred,
        "actions": {"apply_stage": True, "apply_all": True, "rollback": True},
        "stages": stages,
        "target": {
            "production_tph": round(target, 3),
            "delta_pct": round(delta_pct_calc, 3) if delta_pct_calc is not None else None,
            "delta_abs": round(delta_abs_calc, 3) if delta_abs_calc is not None else None,
            "requested": {
                "direction": req.direction,
                "delta_pct": req.delta_pct,
                "delta_abs": req.delta_abs,
                "target_tph": req.target_tph,
                "steps": req.steps
            }
        },
        "match_info": {"candidates_used": 391},
        "targets": {k: (round(v, 3) if isinstance(v, (int, float)) else v) for k, v in targets.items()},
        "steps_cfg": levers,
    }

    tbl = _plans_table()
    err = None
    if _BQ_ENABLED and tbl:
        stages_bq: List[Dict[str, Any]] = []
        for stg in stages:
            spv = stg.get("setpoints")
            sp_out = _as_json_string(spv) if isinstance(spv, (dict, list)) else spv
            stages_bq.append({
                "name": stg.get("name"),
                "setpoints": sp_out,
                "checks": stg.get("checks"),
            })

        plan_row = {
            "plan_id": plan_id,
            "created_at": created_at,
            "mode": f"load_{direction}",
            "direction": direction,
            "delta_pct": float(delta_pct_calc) if delta_pct_calc is not None else None,
            "steps": int(req.steps) if req.steps else None,
            "snapshot": s,
            "targets": targets,
            "stages": stages_bq,
            "predicted_after": pred,
            "target_tph": float(req.target_tph) if req.target_tph is not None else None,
            "delta_abs": float(req.delta_abs) if req.delta_abs is not None else None,
        }
        err = _bq_insert_flexible(tbl, plan_row)
        _remember_bq_attempt("plan_insert", tbl, list(plan_row.keys()), err)
        if err:
            logging.warning("plans_v2 insert error: %s", err)
    else:
        logging.info("Skipping plan log: BQ_ENABLED=%s, table=%s", _BQ_ENABLED, tbl)

    payload["bq_log"] = {"table": tbl, "insert_error": err}
    return payload

# -------------------------
# Actuation (apply stage) + Actuation Logging
# -------------------------
@app.post("/actuate/apply_stage")
def actuate_apply_stage(request: Request, req: ApplyStageReq = Body(default={})):
    # DEV MODE: require only X-Confirm-Apply: yes from an allowed Origin
    origin = request.headers.get("Origin") or request.headers.get("origin")
    if origin and not _origin_allowed(origin):
        raise HTTPException(status_code=403, detail=f"Origin not allowed: {origin}")
    if not _is_manual_actuation_request(request):
        raise HTTPException(status_code=403, detail="Manual actuation requires X-Confirm-Apply: yes")
    setpts = req.extract_setpoints()
    res = _apply_setpoints_internal(
        setpts=setpts,
        mode=req.mode,
        plan_id=req.plan_id,
        stage_index=req.stage_index,
        stage_name=(req.stage or {}).get("name") if isinstance(req.stage, dict) else None
    )
    return res

@app.post("/actuate/rollback")
def actuate_rollback():
    if USE_MOCK:
        return {"ok": True, "note": "mock: nothing to rollback"}
    return {"ok": True, "note": "Live plant rollback not implemented"}

# -------------------------
# Support endpoints for UI comparison & latest actuation
# -------------------------
@app.get("/actuations/latest")
def actuations_latest():
    if not _ACTS_RECENT:
        raise HTTPException(status_code=404, detail="No recent actuation")
    return _ACTS_RECENT[-1]

@app.get("/compare/latest")
def compare_latest():
    if not _ROUTINE_RECENT:
        raise HTTPException(status_code=404, detail="No routine suggestions yet")

    r = _ROUTINE_RECENT[-1]
    if r.get("applied") and isinstance(r.get("actuation"), dict):
        act = r["actuation"]
        return {
            "mode": "applied",
            "applied_at": act.get("applied_at"),
            "before": act.get("before"),
            "after": act.get("after"),
            "used_snapshot_source": r.get("used_snapshot_source"),
            "used_snapshot_ts": r.get("used_snapshot_ts"),
            "used_snapshot_hash": r.get("used_snapshot_hash"),
        }

    return {
        "mode": "preview",
        "current": r.get("current"),
        "proposed_setpoints": r.get("proposed_setpoints"),
        "used_snapshot_source": r.get("used_snapshot_source"),
        "used_snapshot_ts": r.get("used_snapshot_ts"),
        "used_snapshot_hash": r.get("used_snapshot_hash"),
        "reason": r.get("reason"),
        "reason_detail": r.get("reason_detail"),
        "per_lever": r.get("per_lever"),
    }

# -------------------------
# /ingest → BigQuery (snapshots base table)
# -------------------------
@app.post("/ingest")
def ingest(doc: dict = Body(default={})):
    global _LAST_INGEST_RUN
    _LAST_INGEST_RUN = _now_ts()
    if not _BQ_ENABLED or _bq_client is None:
        raise HTTPException(status_code=500, detail=_BQ_ERR or "BigQuery unavailable")

    try:
        snap = doc.get("snapshot") or snapshot()
        required = [
            "production_tph", "kiln_feed_tph", "separator_dp_pa",
            "id_fan_flow_Nm3_h", "cooler_airflow_Nm3_h",
            "kiln_speed_rpm", "o2_percent", "specific_power_kwh_per_ton",
        ]
        for k in required:
            if k not in snap:
                raise HTTPException(status_code=400, detail=f"snapshot missing field: {k}")

        table = _snapshots_table()
        try:
            tbl_obj = _bq_client.get_table(table)  # type: ignore
            raw_field_type = next((f.field_type for f in tbl_obj.schema if f.name == "raw"), None)
        except Exception:
            raw_field_type = None

        skip_raw = os.getenv("SKIP_RAW") in ("1", "true", "yes")

        row: Dict[str, Any] = {
            "ts": _now_ts().isoformat(),
            "source": doc.get("source", "scheduler"),
            "production_tph": float(snap["production_tph"]),
            "kiln_feed_tph": float(snap["kiln_feed_tph"]),
            "separator_dp_pa": float(snap["separator_dp_pa"]),
            "id_fan_flow_Nm3_h": float(snap["id_fan_flow_Nm3_h"]),
            "cooler_airflow_Nm3_h": float(snap["cooler_airflow_Nm3_h"]),
            "kiln_speed_rpm": float(snap["kiln_speed_rpm"]),
            "o2_percent": float(snap["o2_percent"]),
            "specific_power_kwh_per_ton": float(snap["specific_power_kwh_per_ton"]),
        }
        if not skip_raw:
            row["raw"] = _normalize_json_for_field(snap, raw_field_type)

        errors = _bq_client.insert_rows_json(table, [row])  # type: ignore
        if errors:
            msg = json.dumps(errors)
            need_sql_fallback = ("not a record" in msg.lower()) or ("invalid" in msg.lower())

            from google.cloud import bigquery  # type: ignore
            if not need_sql_fallback:
                raise HTTPException(status_code=500, detail=f"BigQuery insert failed: {errors}")

            if skip_raw:
                sql = f"""
                    INSERT INTO `{table}` (
                      ts, source, production_tph, kiln_feed_tph, separator_dp_pa,
                      id_fan_flow_Nm3_h, cooler_airflow_Nm3_h, kiln_speed_rpm,
                      o2_percent, specific_power_kwh_per_ton
                    )
                    VALUES (@ts,@source,@p,@kf,@dp,@idf,@caf,@ks,@o2,@sp)
                """
                params = [
                    bigquery.ScalarQueryParameter("ts", "TIMESTAMP", _now_ts()),
                    bigquery.ScalarQueryParameter("source", "STRING", doc.get("source", "scheduler")),
                    bigquery.ScalarQueryParameter("p", "FLOAT64", float(snap["production_tph"])),
                    bigquery.ScalarQueryParameter("kf", "FLOAT64", float(snap["kiln_feed_tph"])),
                    bigquery.ScalarQueryParameter("dp", "FLOAT64", float(snap["separator_dp_pa"])),
                    bigquery.ScalarQueryParameter("idf", "FLOAT64", float(snap["id_fan_flow_Nm3_h"])),
                    bigquery.ScalarQueryParameter("caf", "FLOAT64", float(snap["cooler_airflow_Nm3_h"])),
                    bigquery.ScalarQueryParameter("ks", "FLOAT64", float(snap["kiln_speed_rpm"])),
                    bigquery.ScalarQueryParameter("o2", "FLOAT64", float(snap["o2_percent"])),
                    bigquery.ScalarQueryParameter("sp", "FLOAT64", float(snap["specific_power_kwh_per_ton"])),
                ]
            else:
                sql = f"""
                    INSERT INTO `{table}` (
                      ts, source, production_tph, kiln_feed_tph, separator_dp_pa,
                      id_fan_flow_Nm3_h, cooler_airflow_Nm3_h, kiln_speed_rpm,
                      o2_percent, specific_power_kwh_per_ton, raw
                    )
                    VALUES (@ts,@source,@p,@kf,@dp,@idf,@caf,@ks,@o2,@sp,@raw)
                """
                params = [
                    bigquery.ScalarQueryParameter("ts", "TIMESTAMP", _now_ts()),
                    bigquery.ScalarQueryParameter("source", "STRING", doc.get("source", "scheduler")),
                    bigquery.ScalarQueryParameter("p", "FLOAT64", float(snap["production_tph"])),
                    bigquery.ScalarQueryParameter("kf", "FLOAT64", float(snap["kiln_feed_tph"])),
                    bigquery.ScalarQueryParameter("dp", "FLOAT64", float(snap["separator_dp_pa"])),
                    bigquery.ScalarQueryParameter("idf", "FLOAT64", float(snap["id_fan_flow_Nm3_h"])),
                    bigquery.ScalarQueryParameter("caf", "FLOAT64", float(snap["cooler_airflow_Nm3_h"])),
                    bigquery.ScalarQueryParameter("ks", "FLOAT64", float(snap["kiln_speed_rpm"])),
                    bigquery.ScalarQueryParameter("o2", "FLOAT64", float(snap["o2_percent"])),
                    bigquery.ScalarQueryParameter("sp", "FLOAT64", float(snap["specific_power_kwh_per_ton"])),
                    bigquery.ScalarQueryParameter("raw", "JSON", _normalize_json_for_field(snap, "JSON")),
                ]
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            job = _bq_client.query(sql, job_config=job_config)  # type: ignore
            job.result()
        return {"ok": True, "table": table}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"/ingest error: {e}")

# -------------------------
# /predict/spower → BQML (with safe fallback)
# -------------------------
@app.post("/predict/spower")
def predict_spower_route(doc: dict = Body(default={})):
    snap = (doc.get("snapshot") or {})
    if not snap:
        if USE_MOCK:
            with _state_lock:
                snap = {k: v for k, v in _STATE.items() if k != "sp"}
        else:
            snap = _latest_snapshot_from_bq()

    def f(x):
        try:
            return float(x)
        except Exception:
            return None

    params = {
        "production_tph":       f(snap.get("production_tph")),
        "kiln_feed_tph":        f(snap.get("kiln_feed_tph")),
        "separator_dp_pa":      f(snap.get("separator_dp_pa")),
        "id_fan_flow_Nm3_h":    f(snap.get("id_fan_flow_Nm3_h")),
        "cooler_airflow_Nm3_h": f(snap.get("cooler_airflow_Nm3_h")),
        "kiln_speed_rpm":       f(snap.get("kiln_speed_rpm")),
        "o2_percent":           f(snap.get("o2_percent")),
    }

    if not _BQ_ENABLED or _bq_client is None:
        pred = {"predicted_specific_power_kwh_per_ton": predict_specific_power({**snap})}
        return {"input": params, "prediction": pred, "note": "BQ disabled; mock prediction"}

    from google.cloud import bigquery  # type: ignore
    sql = f"""
      SELECT * FROM ML.PREDICT(MODEL `{_bq_model_fqn()}`,
        (SELECT
          @production_tph        AS production_tph,
          @kiln_feed_tph         AS kiln_feed_tph,
          @separator_dp_pa       AS separator_dp_pa,
          @id_fan_flow_Nm3_h     AS id_fan_flow_Nm3_h,
          @cooler_airflow_Nm3_h  AS cooler_airflow_Nm3_h,
          @kiln_speed_rpm        AS kiln_speed_rpm,
          @o2_percent            AS o2_percent
        )
      )
    """
    job = _bq_client.query(
        sql,
        location=BQ_LOCATION,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("production_tph", "FLOAT64", params["production_tph"]),
                bigquery.ScalarQueryParameter("kiln_feed_tph", "FLOAT64", params["kiln_feed_tph"]),
                bigquery.ScalarQueryParameter("separator_dp_pa", "FLOAT64", params["separator_dp_pa"]),
                bigquery.ScalarQueryParameter("id_fan_flow_Nm3_h", "FLOAT64", params["id_fan_flow_Nm3_h"]),
                bigquery.ScalarQueryParameter("cooler_airflow_Nm3_h", "FLOAT64", params["cooler_airflow_Nm3_h"]),
                bigquery.ScalarQueryParameter("kiln_speed_rpm", "FLOAT64", params["kiln_speed_rpm"]),
                bigquery.ScalarQueryParameter("o2_percent", "FLOAT64", params["o2_percent"]),
            ]
        ),
    )
    rows = list(job.result())
    pred = dict(rows[0]) if rows else {}
    return {"input": params, "prediction": pred}

# -------------------------
# NEW: Predictive Maintenance helpers + endpoints (generic HI)
# -------------------------
def _df_recent(minutes:int=240, limit:int=5000) -> pd.DataFrame:
    """
    Returns a DataFrame indexed by ts with numeric columns.
    Works on mock (_HIST) or BigQuery snapshots base table.
    """
    if USE_MOCK:
        cutoff = _now_ts() - datetime.timedelta(minutes=minutes)
        rows = [p for p in list(_HIST) if datetime.datetime.fromisoformat(p["ts"].replace("Z","")).astimezone(datetime.timezone.utc) >= cutoff]
        df = pd.DataFrame(rows[-limit:])
    else:
        if not _BQ_ENABLED or _bq_client is None:
            return pd.DataFrame()
        table = _snapshots_table()
        sql = f"""
          SELECT ts,
                 production_tph, kiln_feed_tph, separator_dp_pa,
                 id_fan_flow_Nm3_h, cooler_airflow_Nm3_h,
                 kiln_speed_rpm, o2_percent, specific_power_kwh_per_ton
          FROM `{table}`
          WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @m MINUTE)
          ORDER BY ts ASC
          LIMIT @lim
        """
        from google.cloud import bigquery  # type: ignore
        job = _bq_client.query(
            sql, location=BQ_LOCATION,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("m", "INT64", minutes),
                    bigquery.ScalarQueryParameter("lim", "INT64", limit),
                ]
            )
        )
        rows = list(job.result())
        df = pd.DataFrame([dict(r) for r in rows])
    if df.empty:
        return df
    # normalize ts → datetime
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"])
        df = df.sort_values("ts")
        df = df.set_index("ts")
    # keep numeric
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _pm_select_signals(df: pd.DataFrame) -> pd.DataFrame:
    keys = ["vibration","vibe","accel","bearing","brg","temp","temperature",
            "motor","current","amp","amps","load","rpm","speed","pressure","press",
            "fan","kiln","separator","cooler","o2","specific_power"]
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    cols = [c for c in num_cols if any(k in str(c).lower() for k in keys)]
    return df[cols] if cols else df[num_cols[:6]]

def _pm_compute(df: pd.DataFrame):
    sel = _pm_select_signals(df)
    if sel.empty: 
        return pd.Series(dtype=float), 0.0, pd.DataFrame()
    win = max(10, min(500, int(len(sel)*0.05)))
    roll_mean = sel.rolling(win, min_periods=max(5, win//3)).mean()
    roll_std  = sel.rolling(win, min_periods=max(5, win//3)).std()
    z = (sel - roll_mean) / (roll_std + 1e-9)
    hi = z.abs().mean(axis=1)   # Health Index
    mad = (hi - hi.median()).abs().median()
    thresh = hi.median() + 3.5 * (mad if mad > 0 else hi.std())
    anomalies = sel[hi > thresh]
    return hi, float(thresh if np.isfinite(thresh) else 0.0), anomalies

class PMHealthPoint(BaseModel):
    ts: str
    health_index: float
    threshold: float

class PMAnomaly(BaseModel):
    ts: str
    signals: Dict[str, float]

@app.get("/pm/health_index", response_model=List[PMHealthPoint])
def pm_health_index(minutes: int = Query(default=240, ge=10, le=24*60)):
    df = _df_recent(minutes=minutes)
    if df.empty:
        return []
    hi, thresh, _ = _pm_compute(df)
    out = [{"ts": ts.isoformat(), "health_index": float(v), "threshold": float(thresh)} for ts, v in hi.dropna().items()]
    return out

@app.get("/pm/anomalies", response_model=List[PMAnomaly])
def pm_anomalies(minutes: int = Query(default=240, ge=10, le=24*60), max_rows:int = Query(default=200, ge=1, le=2000)):
    df = _df_recent(minutes=minutes)
    if df.empty:
        return []
    _, _, anomalies = _pm_compute(df)
    out: List[PMAnomaly] = []
    for ts, row in anomalies.tail(max_rows).iterrows():
        sigs = {k: float(row[k]) for k in row.index if pd.notna(row[k])}
        out.append(PMAnomaly(ts=ts.isoformat(), signals=sigs))
    return out

# -------------------------
# NEW: READ APIs for KPI predictions tables
# -------------------------
class KPILatestPoint(BaseModel):
    target_ts: str
    y_hat: float
    y_lo: Optional[float] = None
    y_hi: Optional[float] = None
    kpi_name: Optional[str] = None
    horizon_min: Optional[int] = None
    pred_run_ts: Optional[str] = None

class KPIFuturePoint(BaseModel):
    pred_ts: str
    target_ts: str
    y_hat: float
    kpi_name: Optional[str] = None
    horizon_min: Optional[int] = None

@app.get("/kpi/predictions/latest", response_model=List[KPILatestPoint])
def kpi_predictions_latest(
    kpi_name: str = Query(..., description="KPI name, e.g. 'specific_power_kwh_per_ton'"),
    horizon_min: int = Query(..., ge=1, le=24*60),
    limit: int = Query(2880, ge=1, le=100000),
):
    # Memory fallback
    mem = _PRED_LATEST_MEM.get((kpi_name, horizon_min))
    if mem and (not _BQ_ENABLED or _bq_client is None):
        return mem[:limit]

    if not _BQ_ENABLED or _bq_client is None:
        raise HTTPException(status_code=503, detail=_BQ_ERR or "BigQuery unavailable")

    from google.cloud import bigquery  # type: ignore
    table = _kpi_pred_latest_table()
    sql = f"""
      SELECT target_ts, y_hat, y_lo, y_hi, kpi_name, horizon_min, pred_run_ts
      FROM `{table}`
      WHERE kpi_name = @k AND horizon_min = @h
      ORDER BY target_ts
      LIMIT @lim
    """
    rows = _bq_query(sql, params=[
        bigquery.ScalarQueryParameter("k", "STRING", kpi_name),
        bigquery.ScalarQueryParameter("h", "INT64", horizon_min),
        bigquery.ScalarQueryParameter("lim", "INT64", limit),
    ])
    out: List[KPILatestPoint] = []
    for r in rows:
        d = dict(r)
        out.append(KPILatestPoint(
            target_ts = (d.get("target_ts").isoformat() if isinstance(d.get("target_ts"), datetime.datetime) else str(d.get("target_ts"))),
            y_hat = float(d.get("y_hat")) if d.get("y_hat") is not None else None,  # type: ignore
            y_lo  = float(d.get("y_lo"))  if d.get("y_lo")  is not None else None,  # type: ignore
            y_hi  = float(d.get("y_hi"))  if d.get("y_hi")  is not None else None,  # type: ignore
            kpi_name = d.get("kpi_name"),
            horizon_min = int(d.get("horizon_min")) if d.get("horizon_min") is not None else None,
            pred_run_ts = (d.get("pred_run_ts").isoformat() if isinstance(d.get("pred_run_ts"), datetime.datetime) else (str(d.get("pred_run_ts")) if d.get("pred_run_ts") else None)),
        ))
    return out

@app.get("/kpi/predictions/future", response_model=List[KPIFuturePoint])
def kpi_predictions_future(
    kpi_name: str = Query(..., description="KPI name, e.g. 'production_tph'"),
    limit: int = Query(500, ge=1, le=100000),
):
    # Memory fallback
    mem = _PRED_FUTURE_MEM.get(kpi_name)
    if mem and (not _BQ_ENABLED or _bq_client is None):
        return mem[:limit]

    if not _BQ_ENABLED or _bq_client is None:
        raise HTTPException(status_code=503, detail=_BQ_ERR or "BigQuery unavailable")

    from google.cloud import bigquery  # type: ignore
    table = _kpi_pred_future_table()
    sql = f"""
      SELECT pred_ts, target_ts, y_hat, kpi_name, horizon_min
      FROM `{table}`
      WHERE kpi_name = @k
      ORDER BY pred_ts DESC, target_ts ASC
      LIMIT @lim
    """
    rows = _bq_query(sql, params=[
        bigquery.ScalarQueryParameter("k", "STRING", kpi_name),
        bigquery.ScalarQueryParameter("lim", "INT64", limit),
    ])
    out: List[KPIFuturePoint] = []
    for r in rows:
        d = dict(r)
        out.append(KPIFuturePoint(
            pred_ts   = (d.get("pred_ts").isoformat() if isinstance(d.get("pred_ts"), datetime.datetime) else str(d.get("pred_ts"))),
            target_ts = (d.get("target_ts").isoformat() if isinstance(d.get("target_ts"), datetime.datetime) else str(d.get("target_ts"))),
            y_hat     = float(d.get("y_hat")) if d.get("y_hat") is not None else None,  # type: ignore
            kpi_name  = d.get("kpi_name"),
            horizon_min = int(d.get("horizon_min")) if d.get("horizon_min") is not None else None,
        ))
    return out

# -------------------------
# NEW: Condition Monitoring KPI models and helpers (VHI + MCI)
# -------------------------
class VHIPoint(BaseModel):
    ts: str
    health_index: float
    threshold: float
    status: str  # "ok" | "watch" | "alert"

class MCIPoint(BaseModel):
    ts: str
    value: float
    kpi: str  # "MCI_percent" or "BearingTempRise_C"

# ---- Robust column matching (improved) ----
def _pick_col(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    """
    Find a column by exact/loose name match (case-insensitive):
    - exact
    - underscore-insensitive exact
    - startswith
    - contains
    """
    cols = list(df.columns)
    low = [c.lower() for c in cols]
    for n in names:
        nlow = n.lower()
        if nlow in low:
            return cols[low.index(nlow)]
        for i, cl in enumerate(low):
            if cl.replace("_", "") == nlow.replace("_", ""):
                return cols[i]
        for i, cl in enumerate(low):
            if cl.startswith(nlow) or (nlow in cl):
                return cols[i]
    return None

def _select_vibration_cols(df: pd.DataFrame) -> List[str]:
    keys = ["vibration", "vibe", "accel", "acceleration", "bearing", "brg"]
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    vib_cols = [c for c in num_cols if any(k in str(c).lower() for k in keys)]
    return vib_cols

def _compute_vhi(df: pd.DataFrame):
    vib_cols = _select_vibration_cols(df)
    if not vib_cols:
        return pd.Series(dtype=float), 0.0, pd.DataFrame()
    sel = df[vib_cols].copy()
    win = max(10, min(500, int(len(sel) * 0.05)))
    roll_mean = sel.rolling(win, min_periods=max(5, win // 3)).mean()
    roll_std  = sel.rolling(win, min_periods=max(5, win // 3)).std()
    z = (sel - roll_mean) / (roll_std + 1e-9)
    hi = z.abs().mean(axis=1)  # VHI
    mad = (hi - hi.median()).abs().median()
    thresh = hi.median() + 3.5 * (mad if mad > 0 else (hi.std() if np.isfinite(hi.std()) else 0.0))
    anomalies = sel[hi > thresh]
    return hi, float(thresh if np.isfinite(thresh) else 0.0), anomalies

def _status_from_hi(hi_val: float, thresh: float) -> str:
    if thresh is None or not np.isfinite(thresh) or thresh <= 0:
        return "ok"
    if hi_val < 0.5 * thresh:
        return "ok"
    if hi_val < 1.0 * thresh:
        return "watch"
    return "alert"

def _compute_mci_series(df: pd.DataFrame) -> pd.Series:
    # broadened to catch motor_current_a/b/c too
    cand_a = ["ia","i_a","phase_a","phase a","current_a","current a","motor_current_a","motor current a"]
    cand_b = ["ib","i_b","phase_b","phase b","current_b","current b","motor_current_b","motor current b"]
    cand_c = ["ic","i_c","phase_c","phase c","current_c","current c","motor_current_c","motor current c"]
    ca = _pick_col(df, cand_a + [s.upper() for s in cand_a])
    cb = _pick_col(df, cand_b + [s.upper() for s in cand_b])
    cc = _pick_col(df, cand_c + [s.upper() for s in cand_c])
    if not ca or not cb or not cc:
        return pd.Series(index=df.index, dtype=float)
    sA = pd.to_numeric(df[ca], errors="coerce")
    sB = pd.to_numeric(df[cb], errors="coerce")
    sC = pd.to_numeric(df[cc], errors="coerce")
    avg = (sA + sB + sC) / 3.0
    mci = (pd.concat([sA,sB,sC], axis=1).max(axis=1) - pd.concat([sA,sB,sC], axis=1).min(axis=1)) / (avg + 1e-9) * 100.0
    mci[(avg <= 0) | (~np.isfinite(mci))] = np.nan
    return mci

def _compute_bearing_temp_rise_series(df: pd.DataFrame) -> pd.Series:
    # accept _C suffixed and common variants
    cand_brg = ["bearing_temp","bearing_temp_c","brg_temp","brg_temp_c","bearing temperature","bearing"]
    cand_inl = ["suction_temp","suction_temp_c","inlet_temp","inlet_temp_c","inlet temperature","suction temperature"]
    cand_amb = ["ambient_temp","ambient_temp_c","ambient temperature","room_temp","room temperature"]
    cb = _pick_col(df, cand_brg + [s.upper() for s in cand_brg])
    ci = _pick_col(df, cand_inl + [s.upper() for s in cand_inl])
    ca = _pick_col(df, cand_amb + [s.upper() for s in cand_amb])
    if not cb:
        return pd.Series(index=df.index, dtype=float)
    b = pd.to_numeric(df[cb], errors="coerce")
    base = None
    if ci:
        base = pd.to_numeric(df[ci], errors="coerce")
    elif ca:
        base = pd.to_numeric(df[ca], errors="coerce")
    else:
        return pd.Series(index=df.index, dtype=float)
    return b - base

# -------------------------
# >>> VHI SHIMS so Pylance is happy (only used if your project didn't define them)
# -------------------------
if "_compute_vhi_series" not in globals():
    def _compute_vhi_series(df: pd.DataFrame) -> pd.Series:
        """
        Fallback: compute a Vibration Health Index time series from vibration-ish numeric columns.
        Robust rolling z-scores averaged across selected columns.
        """
        cols = _select_vibration_cols(df)
        if not cols:
            # Consider generic numeric cols as a very last resort
            num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            cols = num_cols
        if not cols:
            return pd.Series(index=getattr(df, "index", []), dtype=float)

        sel = df[cols].copy()
        win = max(10, min(500, int(len(sel) * 0.05))) or 10
        roll_mean = sel.rolling(win, min_periods=max(5, win // 3)).mean()
        roll_std  = sel.rolling(win, min_periods=max(5, win // 3)).std()
        z = (sel - roll_mean) / (roll_std + 1e-9)
        hi = z.abs().mean(axis=1)
        return hi

if "_vhi_threshold" not in globals():
    def _vhi_threshold(vhi: pd.Series) -> float:
        """
        Fallback: robust threshold as median + 3*MAD, with a floor at P95 to avoid being too low.
        """
        v = pd.to_numeric(vhi, errors="coerce").dropna()
        if v.empty:
            return float("nan")
        med = float(np.median(v.values))
        mad = float(np.median(np.abs(v.values - med)))
        robust_thr = med + 3.0 * (1.4826 * mad)
        p95 = float(np.percentile(v.values, 95))
        return max(robust_thr, p95)

# -------------------------
# VHI + MCI endpoints
# -------------------------
@app.get("/cm/vhi", response_model=List[VHIPoint])
def cm_vhi(minutes: int = Query(default=240, ge=10, le=24*60)):
    """
    Vibration Health Index over the window. Higher is worse.
    Threshold is robust (median+3.5*MAD). Status bands:
      ok (<0.5*thr), watch (0.5*thr..thr), alert (>=thr).
    """
    df = _df_recent(minutes=minutes)
    if df.empty:
        return []
    hi, thresh, _ = _compute_vhi(df)
    out: List[VHIPoint] = []
    for ts, v in hi.dropna().items():
        out.append(VHIPoint(
            ts=ts.isoformat(),
            health_index=float(v),
            threshold=float(thresh),
            status=_status_from_hi(float(v), float(thresh)),
        ))
    return out

@app.post("/cm/vhi/log")
def cm_vhi_log(source: str = Body(default="scheduler")):
    """
    Compute latest VHI point and append to cm_kpis (if BQ enabled).
    Adds aux.status and aux.threshold so segments & UI can classify it.
    """
    df = _df_recent(minutes=240)
    if df.empty:
        raise HTTPException(status_code=404, detail="No data for VHI")

    # Compute series
    vhi = _compute_vhi_series(df)  # shim ensures this exists
    vhi = vhi.dropna()
    if vhi.empty:
        raise HTTPException(status_code=404, detail="VHI not computable")

    # Threshold (prefer existing helper if present; else robust fallback)
    try:
        thr = float(_vhi_threshold(vhi))  # shim ensures this exists
    except Exception:
        med = float(np.median(vhi.values))
        mad = float(np.median(np.abs(vhi.values - med)))
        thr = med + 3.0 * (1.4826 * mad) if np.isfinite(med) and np.isfinite(mad) else float("nan")

    val_ts = vhi.index[-1]
    val = float(vhi.iloc[-1])

    # Status bands
    def _vhi_status(v: float, t: float) -> str:
        if not np.isfinite(v):
            return "ok"
        if np.isfinite(t):
            if v < 0.7 * t:
                return "ok"
            if v < 1.0 * t:
                return "watch"
            return "alert"
        if v < 0.8:
            return "ok"
        if v < 1.2:
            return "watch"
        return "alert"

    status = _vhi_status(val, thr)

    # ---- BigQuery insert ----
    tbl_name = _cm_table() or os.getenv("CM_KPIS_TABLE")
    if not tbl_name:
        raise HTTPException(status_code=500, detail="CM KPIs table not configured")
    tbl = f"`{tbl_name.strip().strip('`')}`"

    err = None
    if _BQ_ENABLED and _bq_client is not None and tbl:
        payload = {
            "ts": val_ts.isoformat(),
            "kpi": "VHI",
            "value": val,
            "aux": {"status": status, "threshold": thr if np.isfinite(thr) else None},
            "source": source,
        }
        err = _bq_insert_flexible(tbl, payload)
        _remember_bq_attempt("cm_kpi_insert", tbl, ["ts", "kpi", "value", "aux", "source"], err)

    return {
        "ok": err is None if _BQ_ENABLED else True,
        "table": tbl_name,
        "point": {
            "ts": val_ts.isoformat(),
            "kpi": "VHI",
            "value": val,
            "threshold": thr if np.isfinite(thr) else None,
            "status": status,
        },
        "bq_error": err,
    }

@app.get("/cm/mci", response_model=List[MCIPoint])
def cm_mci(minutes: int = Query(default=240, ge=10, le=24*60)):
    """
    Computes time series for:
      - MCI (%) if Ia/Ib/Ic present
      - else Bearing Temp Rise (°C) as fallback
    """
    df = _df_recent(minutes=minutes)
    if df.empty:
        return []
    mci = _compute_mci_series(df)
    if mci.notna().sum() >= max(3, int(0.05 * len(df))):
        kpi_name = "MCI_percent"
        series = mci
    else:
        kpi_name = "BearingTempRise_C"
        series = _compute_bearing_temp_rise_series(df)

    out: List[MCIPoint] = []
    for ts, v in series.dropna().items():
        out.append(MCIPoint(ts=ts.isoformat(), value=float(v), kpi=kpi_name))
    return out

@app.post("/cm/mci/log")
def cm_mci_log(source: str = Body(default="scheduler")):
    """
    Compute latest MCI (or Bearing Temp Rise) point and append to cm_kpis table (if BQ enabled).
    Adds aux.status so segments can classify it.
    """
    df = _df_recent(minutes=240)
    if df.empty:
        raise HTTPException(status_code=404, detail="No data for MCI")

    mci = _compute_mci_series(df)
    use_mci = mci.notna().sum() >= max(3, int(0.05 * len(df)))

    if use_mci:
        kpi_name = "MCI_percent"
        series = mci

        def _mci_status(val: float) -> str:
            if not np.isfinite(val):
                return "ok"
            if val < 30.0:
                return "ok"
            if val < 50.0:
                return "watch"
            return "alert"

        status_fn = _mci_status
        aux_extra = {}
    else:
        kpi_name = "BearingTempRise_C"
        series = _compute_bearing_temp_rise_series(df)

        def _btr_status(val: float) -> str:
            if not np.isfinite(val):
                return "ok"
            if val < 10.0:
                return "ok"
            if val < 20.0:
                return "watch"
            return "alert"

        status_fn = _btr_status
        aux_extra = {"note": "fallback_no_three_phase_currents"}

    series = series.dropna()
    if series.empty:
        raise HTTPException(status_code=404, detail="MCI/TempRise not computable")

    ts = series.index[-1]
    val = float(series.iloc[-1])
    status = status_fn(val)

    tbl = _cm_table()
    err = None
    if _BQ_ENABLED and _bq_client is not None and tbl:
        payload = {
            "ts": ts.isoformat(),
            "kpi": kpi_name,
            "value": val,
            "aux": {"status": status, **aux_extra},
            "source": source,
        }
        err = _bq_insert_flexible(tbl, payload)
        _remember_bq_attempt("cm_kpi_insert", tbl, ["ts", "kpi", "value", "aux", "source"], err)

    return {
        "ok": err is None if _BQ_ENABLED else True,
        "table": tbl,
        "point": {"ts": ts.isoformat(), "kpi": kpi_name, "value": val, "status": status},
        "bq_error": err,
    }

# -------------------------
# /metrics & debug
# -------------------------
@app.get("/metrics")
def metrics():
    if USE_MOCK:
        with _state_lock:
            s = {k: v for k, v in _STATE.items() if k != "sp"}
        history_len = len(_HIST)
    else:
        s = {}
        history_len = 0
    cfg = get_config()
    return {
        "version": SERVICE_VERSION,
        "mock": bool(USE_MOCK),
        "tick_sec": MOCK_TICK_SEC,
        "apply_enabled": bool(APPLY_ENABLED),
        "history_points": history_len,
        "snapshot": s,
        "levers": list(cfg.get("levers", {}).keys()),
        "bq_enabled": _BQ_ENABLED,
        "sched_period_sec": SCHED_PERIOD_SEC,
        "thresholds": {
            "MIN_PCT_DELTA": MIN_PCT_DELTA,
            "MIN_ABS_ID_FAN": MIN_ABS_ID_FAN,
            "MIN_ABS_COOLER": MIN_ABS_COOLER,
        },
        "cron_defaults": {
            "CRON_APPLY_TOP": CRON_APPLY_TOP,
            "CRON_NUDGE_IF_NEUTRAL": CRON_NUDGE_IF_NEUTRAL,
            "CRON_LOG_SUGGESTIONS": CRON_LOG_SUGGESTIONS,
        },
        "spower": {
            "mode": SPOWER_MODE,
            "tol": SPOWER_TOL,
            "logging": bool(LOG_PHYSICS),
        }
    }

@app.get("/debug/physics_flags")
def physics_flags():
    sp = _STATE.get("sp", {})
    drv = _STATE.get("_spower_drivers", {})
    return {
        "spower_mode": SPOWER_MODE,
        "spower_tol": SPOWER_TOL,
        "log_physics": LOG_PHYSICS,
        "current_drivers": {
            "production_tph": _STATE.get("production_tph"),
            "separator_dp_pa_sp": sp.get("separator_dp_pa"),
            "o2_percent": _STATE.get("o2_percent"),
        },
        "last_drivers_seen": drv,
    }

# -------------------------
# Forecasting utilities & loop (NEW)
# -------------------------
def _collect_series(kpi: str, minutes:int=240, limit:int=5000):
    df = _df_recent(minutes=minutes, limit=limit)
    if df.empty or kpi not in df.columns:
        return [], []
    s = pd.to_numeric(df[kpi], errors="coerce").dropna()
    if s.empty:
        return [], []
    ts = list(s.index.to_pydatetime())
    ys = list(map(float, s.values))
    return ts, ys

def _fit_lr_forecast(ts_list: List[datetime.datetime], y_list: List[float],
                     horizon_min: int, step_min: int) -> List[Dict[str, Any]]:
    if len(ts_list) < 5:
        return []
    t0 = ts_list[0]
    x = np.array([(t - t0).total_seconds()/60.0 for t in ts_list], dtype=float)
    y = np.array(y_list, dtype=float)
    msk = np.isfinite(x) & np.isfinite(y)
    x, y = x[msk], y[msk]
    if len(x) < 5:
        return []
    slope, intercept = np.polyfit(x, y, 1)
    last_t = ts_list[-1]
    steps = max(1, int(np.ceil(horizon_min/step_min)))
    pts = []
    for i in range(1, steps+1):
        dtm = i*step_min
        xx = ((last_t + datetime.timedelta(minutes=dtm)) - t0).total_seconds()/60.0
        yhat = float(intercept + slope*xx)
        pts.append({"target_ts": (last_t + datetime.timedelta(minutes=dtm)).isoformat(),
                    "y_hat": yhat})
    return pts

def _write_preds_bq(kpi: str, horizon: int, latest_pts: List[Dict[str, Any]],
                    pred_run_ts: datetime.datetime):
    tbl_latest = _kpi_pred_latest_table() if _BQ_ENABLED else None
    tbl_future = _kpi_pred_future_table() if _BQ_ENABLED else None
    err1 = err2 = None
    if _BQ_ENABLED and tbl_latest and latest_pts:
        for p in latest_pts:
            row = {
                "target_ts": p["target_ts"],
                "y_hat": p["y_hat"],
                "y_lo": None,
                "y_hi": None,
                "kpi_name": kpi,
                "horizon_min": horizon,
                "pred_run_ts": pred_run_ts.isoformat(),
            }
            e = _bq_insert_flexible(tbl_latest, row)
            if e and err1 is None: err1 = e
    if _BQ_ENABLED and tbl_future and latest_pts:
        pred_ts = pred_run_ts.isoformat()
        for p in latest_pts:
            row = {
                "pred_ts": pred_ts,
                "target_ts": p["target_ts"],
                "y_hat": p["y_hat"],
                "kpi_name": kpi,
                "horizon_min": horizon,
            }
            e = _bq_insert_flexible(tbl_future, row)
            if e and err2 is None: err2 = e
    return err1 or err2

def _forecast_loop():
    while True:
        try:
            if not FORECAST_PM_ENABLE:
                time.sleep(max(5, FORECAST_PM_INTERVAL_SEC))
                continue
            pred_run_ts = _now_ts()
            for kpi in PM_KPIS:
                ts_list, y_list = _collect_series(kpi, minutes=max(240, FORECAST_PM_HORIZON_MIN*2))
                pts = _fit_lr_forecast(ts_list, y_list, FORECAST_PM_HORIZON_MIN, FORECAST_PM_STEP_MIN)
                _mem_pred_latest_write(kpi, FORECAST_PM_HORIZON_MIN, pts, pred_run_ts)
                _mem_pred_future_write(kpi, FORECAST_PM_HORIZON_MIN, pts, pred_run_ts)
                _write_preds_bq(kpi, FORECAST_PM_HORIZON_MIN, pts, pred_run_ts)
        except Exception as e:
            logging.warning("forecast loop error: %s", e)
        time.sleep(max(10, FORECAST_PM_INTERVAL_SEC))

# -------------------------
# Startup diagnostics & background starters
# -------------------------
@app.on_event("startup")
def _log_routes_on_startup():
    try:
        for r in app.routes:
            methods = getattr(r, "methods", None)
            logging.info("ROUTE path=%s methods=%s name=%s", r.path, methods, r.name)
    except Exception as e:
        logging.info("Route logging failed: %s", e)

@app.on_event("startup")
def _start_forecaster():
    if FORECAST_PM_ENABLE:
        threading.Thread(target=_forecast_loop, daemon=True).start()
