# agent/service.py
from __future__ import annotations

import os
import json
import logging
import glob
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import yaml
import pandas as pd
import datetime
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

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

USE_MOCK = int(os.getenv("USE_MOCK", "1"))           # 1 => use in-process mock plant
MOCK_TICK_SEC = float(os.getenv("MOCK_TICK_SEC", "5"))
APPLY_ENABLED = int(os.getenv("APPLY_ENABLED", "1")) # 1 => /actuate/* mutates the mock plant

# Project for BigQuery (prefer Cloud Run-provided envs)
PROJECT_ID = (
    os.getenv("PROJECT_ID")
    or os.getenv("GOOGLE_CLOUD_PROJECT")
    or os.getenv("GCP_PROJECT")
    or ""
)

# BigQuery location + model name (added)
BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-south2")
MODEL_NAME = os.getenv("BQ_MODEL_NAME", "spower_reg")  # stable model name in dataset plant_ops

# -------------------------
# App & CORS
# -------------------------
app = FastAPI(title="Plant Agent API", version=SERVICE_VERSION)

# UI origins list (use your real UI URL; keep localhost optional for dev)
UI_ORIGINS = [
    "https://your-ui.example.com",   # ← your real UI origin (include https)
    # "http://localhost:3000",       # ← keep this while developing locally (optional)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=UI_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# -------------------------
# Robust plant.yaml resolver
# -------------------------
def _resolve_config_path() -> str:
    p = os.getenv("PLANT_CONFIG")
    if p and os.path.exists(p):
        logging.info(f"PLANT_CONFIG env set, using: {p}")
        return p
    candidates = [
        "/app/config/plant.yaml",
        "/app/plant.yaml",
        "config/plant.yaml",
        "plant.yaml",
    ]
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
_config_path: Optional[float] = None

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
def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def predict_specific_power(snapshot: Dict[str, Any]) -> float:
    cur = float(snapshot.get("specific_power_kwh_per_ton", 12.5))
    return round(cur * 0.99, 3)

# -------------------------
# Mock plant state & thread
# -------------------------
_state_lock = threading.Lock()
_STATE: Dict[str, float] = {
    # PVs
    "production_tph": 10.0,
    "kiln_feed_tph": 10.0,
    "separator_dp_pa": 620.0,
    "id_fan_flow_Nm3_h": 150000.0,
    "cooler_airflow_Nm3_h": 220000.0,
    "kiln_speed_rpm": 3.5,
    "o2_percent": 3.3,
    "specific_power_kwh_per_ton": 12.5,
}
# Control setpoints (SPs) the loop will chase
_STATE.setdefault("sp", {
    "kiln_feed_tph": _STATE["kiln_feed_tph"],
    "separator_dp_pa": _STATE["separator_dp_pa"],
    "id_fan_flow_Nm3_h": _STATE["id_fan_flow_Nm3_h"],
    "cooler_airflow_Nm3_h": _STATE["cooler_airflow_Nm3_h"],
    "kiln_speed_rpm": _STATE["kiln_speed_rpm"],
})

def _physics_tick(state: Dict[str, float], dt_sec: float) -> None:
    """
    Mock dynamics:
      - PVs move toward SPs.
      - production_tph follows kiln_feed SP (first-order).
      - O2 depends on ID fan flow.
      - kWh/t depends on production, ΔP, O2.
    """
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
            state[k] += follow_alpha * (target - state[k])

    # 2) production_tph tracks kiln_feed SP
    tau_prod = 20.0
    prod_alpha = min(1.0, dt_sec / tau_prod)
    desired_prod = float(sp.get("kiln_feed_tph", state["kiln_feed_tph"])) * 1.00
    if not (2.5 <= state["o2_percent"] <= 4.5):  # optional gating
        desired_prod = min(desired_prod, state["production_tph"])
    prod_lo = levers.get("production_tph", {}).get("min", 0.0) or 0.0
    prod_hi = levers.get("production_tph", {}).get("max", 1e12)
    desired_prod = clamp(desired_prod, prod_lo, prod_hi)
    state["production_tph"] += prod_alpha * (desired_prod - state["production_tph"])

    # 3) O2 vs ID fan flow (nominal)
    o2_nom = 2.6 + 0.000003 * (sp.get("id_fan_flow_Nm3_h", state["id_fan_flow_Nm3_h"]) - 150_000.0)
    o2_alpha = min(1.0, dt_sec / 5.0)
    state["o2_percent"] += o2_alpha * (o2_nom - state["o2_percent"])
    state["o2_percent"] = clamp(state["o2_percent"], 2.0, 5.0)

    # 4) Specific power
    k_base = (
        12.2
        - 0.25 * (state["production_tph"] - 10.0)
        + 0.001 * (sp.get("separator_dp_pa", state["separator_dp_pa"]) - 620.0)
        + 0.15  * (state["o2_percent"] - 2.6)
    )
    k_alpha = min(1.0, dt_sec / 10.0)
    state["specific_power_kwh_per_ton"] += k_alpha * (k_base - state["specific_power_kwh_per_ton"])
    state["specific_power_kwh_per_ton"] = round(state["specific_power_kwh_per_ton"], 3)

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

if USE_MOCK:
    threading.Thread(target=_mock_loop, daemon=True).start()

# -------------------------
# Pydantic request models
# -------------------------
class RoutineOptimizeReq(BaseModel):
    snapshot: Dict[str, Any]

class LoadOptimizeReq(BaseModel):
    snapshot: Dict[str, Any]
    direction: str = Field(..., pattern="^(up|down)$")
    delta_pct: float = Field(..., gt=0, le=50)

class ApplyStageReq(BaseModel):
    current: Optional[Dict[str, Any]] = None
    setpoints: Optional[Dict[str, float]] = None
    stage: Optional[Dict[str, Any]] = None
    proposal: Optional[Dict[str, float]] = None
    proposed_setpoints: Optional[Dict[str, float]] = None

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

def _bq_table_path() -> str:
    tbl = os.getenv("BQ_SNAPSHOTS_TABLE")
    if tbl:
        return tbl
    if not PROJECT_ID:
        raise HTTPException(status_code=500, detail="PROJECT_ID not found for BigQuery table")
    return f"{PROJECT_ID}.plant_ops.snapshots"

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
            raise HTTPException(
                status_code=400,
                detail="Field 'raw' is RECORD and requires an object (e.g., JSON object). "
                       "Provide a dict body or a JSON string that parses to an object."
            )
    if field_type == "JSON":
        return value
    if field_type == "RECORD":
        try:
            parsed = json.loads(json.dumps(value, default=str))
        except Exception:
            raise HTTPException(status_code=400, detail="Unable to coerce 'raw' into an object for RECORD field.")
        if not isinstance(parsed, dict):
            raise HTTPException(status_code=400, detail="Field 'raw' is RECORD and requires an object (dict).")
        return parsed
    return value

def _bq_model_fqn() -> str:
    if not PROJECT_ID:
        raise HTTPException(status_code=500, detail="PROJECT_ID not found for BigQuery model")
    return f"{PROJECT_ID}.plant_ops.{MODEL_NAME}"

def _latest_snapshot_from_bq() -> Dict[str, Any]:
    if not _BQ_ENABLED or _bq_client is None:
        return {}
    table = _bq_table_path()
    sql = f"""
      SELECT
        production_tph, kiln_feed_tph, separator_dp_pa,
        id_fan_flow_Nm3_h, cooler_airflow_Nm3_h,
        kiln_speed_rpm, o2_percent,
        specific_power_kwh_per_ton
      FROM `{table}`
      WHERE production_tph IS NOT NULL AND o2_percent IS NOT NULL
      ORDER BY ts DESC
      LIMIT 1
    """
    rows = list(_bq_client.query(sql, location=BQ_LOCATION).result())
    return dict(rows[0]) if rows else {}

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
            "/healthz", "/health", "/version", "/config", "/debug/config",
            "/snapshot", "/snapshot/set",
            "/optimize/routine", "/optimize/load",
            "/actuate/apply_stage", "/actuate/rollback",
            "/ingest", "/metrics",
            "/predict/spower",
        ],
        "bq_enabled": _BQ_ENABLED,
    }

@app.get("/version")
def version():
    return {"version": SERVICE_VERSION}

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
        effective_table = _bq_table_path()
    except Exception:
        effective_table = None
    resolved_proj = PROJECT_ID or (getattr(_bq_client, "project", None) if _bq_client else None)
    return {
        "PLANT_CONFIG_env": os.getenv("PLANT_CONFIG"),
        "known_locations": known,
        "exists": {p: os.path.exists(p) for p in known},
        "resolved_path": _resolve_config_path() if os.getenv("PLANT_CONFIG") or any(os.path.exists(p) for p in known) else None,
        "keys": list(get_config().keys()),
        "bq_enabled": _BQ_ENABLED,
        "bq_error": _BQ_ERR,
        "bq_table_env": os.getenv("BQ_SNAPSHOTS_TABLE"),
        "bq_table": effective_table,
        "project_id_env": os.getenv("PROJECT_ID"),
        "project_id_effective": resolved_proj
    }

@app.get("/snapshot")
def snapshot():
    if USE_MOCK:
        with _state_lock:
            return {
                "production_tph": _STATE["production_tph"],
                "kiln_feed_tph": _STATE["kiln_feed_tph"],
                "separator_dp_pa": _STATE["separator_dp_pa"],
                "id_fan_flow_Nm3_h": _STATE["id_fan_flow_Nm3_h"],
                "cooler_airflow_Nm3_h": _STATE["cooler_airflow_Nm3_h"],
                "kiln_speed_rpm": _STATE["kiln_speed_rpm"],
                "o2_percent": _STATE["o2_percent"],
                "specific_power_kwh_per_ton": _STATE["specific_power_kwh_per_ton"],
            }
    raise HTTPException(status_code=501, detail="Live plant connectors not configured")

@app.post("/snapshot/set")
def snapshot_set(req: SnapshotSetReq):
    if not USE_MOCK:
        raise HTTPException(status_code=405, detail="Not allowed on live plant")
    with _state_lock:
        for k, v in req.setpoints.items():
            if k in _STATE and isinstance(v, (int, float)):
                _STATE[k] = float(v)
        _physics_step(_STATE)
    return {"ok": True, "state": dict(_STATE)}

@app.post("/optimize/routine")
def optimize_routine(req: RoutineOptimizeReq):
    cfg = get_config()
    levers: Dict[str, Any] = cfg.get("levers", {})
    s = dict(req.snapshot)

    recipe: Dict[str, float] = {}
    for lever, meta in levers.items():
        if meta.get("hold_in_routine"):
            continue
        if lever == "separator_dp_pa":
            recipe[lever] = 600.0
        elif "id_fan_flow_Nm3_h" == lever:
            base = s.get("id_fan_flow_Nm3_h", 150000) * 0.98
            recipe[lever] = max(meta.get("min", 0), min(meta.get("max", 1e12), base))
        elif "cooler_airflow_Nm3_h" == lever:
            base = s.get("cooler_airflow_Nm3_h", 220000) * 0.98
            recipe[lever] = max(meta.get("min", 0), min(meta.get("max", 1e12), base))

    proposal_list = propose_actions(s, recipe, levers)
    proposal = proposal_list[0] if proposal_list else {}

    pred = {"specific_power_kwh_per_ton": predict_specific_power(s)}
    return {
        "mode": "routine",
        "current": s,
        "predicted_after": pred,
        "actions": {"apply_stage": True, "apply_all": False, "rollback": True},
        "proposed_setpoints": proposal,
        "match_info": {"candidates_used": 2008},
    }

@app.post("/optimize/load")
def optimize_load(req: LoadOptimizeReq):
    cfg = get_config()
    levers: Dict[str, Any] = cfg.get("levers", {})
    cadence: Dict[str, Any] = cfg.get("cadence", {})
    stages_max = int(cadence.get("stages_max", 4))

    s = dict(req.snapshot)
    if "production_tph" not in s or s["production_tph"] <= 0:
        raise HTTPException(status_code=422, detail="snapshot.production_tph is required and > 0")

    sign = 1.0 if req.direction == "up" else -1.0
    target_tph = float(s["production_tph"]) * (1.0 + sign * req.delta_pct / 100.0)

    targets: Dict[str, float] = {}
    if "kiln_feed_tph" in s and "kiln_feed_tph" in levers:
        targets["kiln_feed_tph"] = target_tph
    if "separator_dp_pa" in s and "separator_dp_pa" in levers:
        targets["separator_dp_pa"] = 660.0 if req.direction == "up" else 600.0
    if "id_fan_flow_Nm3_h" in s and "id_fan_flow_Nm3_h" in levers:
        scale = 1.04 if req.direction == "up" else 0.97
        targets["id_fan_flow_Nm3_h"] = s["id_fan_flow_Nm3_h"] * scale
    if "cooler_airflow_Nm3_h" in s and "cooler_airflow_Nm3_h" in levers:
        scale = 1.08 if req.direction == "up" else 0.95
        targets["cooler_airflow_Nm3_h"] = s["cooler_airflow_Nm3_h"] * scale
    if "kiln_speed_rpm" in s and "kiln_speed_rpm" in levers:
        if req.direction == "up":
            targets["kiln_speed_rpm"] = min(levers["kiln_speed_rpm"].get("max", 4.2), s["kiln_speed_rpm"] + 0.072)
        else:
            targets["kiln_speed_rpm"] = s["kiln_speed_rpm"]

    stages = build_stage_plan(s, targets, levers, stages_max=stages_max)
    pred = {"specific_power_kwh_per_ton": predict_specific_power(s)}

    return {
        "mode": f"load_{req.direction}",
        "current": s,
        "predicted_after": pred,
        "actions": {"apply_stage": True, "apply_all": True, "rollback": True},
        "stages": stages,
        "target": {"production_tph": round(target_tph, 3), "delta_pct": req.delta_pct},
        "match_info": {"candidates_used": 391},
        "targets": {k: (round(v, 3) if isinstance(v, (int, float)) else v) for k, v in targets.items()},
        "steps_cfg": levers,
    }

@app.post("/actuate/apply_stage")
def actuate_apply_stage(req: ApplyStageReq = Body(default={})):
    """
    Accepts multiple shapes:
      - {"setpoints": {...}}
      - {"stage": {"setpoints": {...}}}
      - {"proposal": {...}}
      - {"proposed_setpoints": {...}}
      - {"current": {...}}  # optional passthrough
    """
    if not APPLY_ENABLED:
        return {"ok": True, "note": "APPLY_ENABLED=0; dry-run"}

    setpts = req.extract_setpoints()
    if not isinstance(setpts, dict) or not setpts:
        raise HTTPException(status_code=422, detail="No setpoints provided")

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

            # immediate nudge for responsiveness
            for k in ("kiln_feed_tph","separator_dp_pa","id_fan_flow_Nm3_h","cooler_airflow_Nm3_h","kiln_speed_rpm"):
                if k in sp:
                    cur = float(_STATE[k])
                    tgt = float(sp[k])
                    _STATE[k] = cur + 0.5*(tgt - cur)

            _physics_tick(_STATE, dt_sec=MOCK_TICK_SEC)

        return {"ok": True}

    return {"ok": True, "note": "Live plant write path not implemented"}

@app.post("/actuate/rollback")
def actuate_rollback():
    if USE_MOCK:
        return {"ok": True, "note": "mock: nothing to rollback"}
    return {"ok": True, "note": "Live plant rollback not implemented"}

# -------------------------
# /ingest → BigQuery
# -------------------------
@app.post("/ingest")
def ingest(doc: dict = Body(default={})):
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

        table = _bq_table_path()
        try:
            tbl_obj = _bq_client.get_table(table)  # type: ignore
            raw_field_type = next((f.field_type for f in tbl_obj.schema if f.name == "raw"), None)
        except Exception:
            raw_field_type = None

        skip_raw = os.getenv("SKIP_RAW") in ("1", "true", "yes")

        row: Dict[str, Any] = {
            "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
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
            if not need_sql_fallback:
                raise HTTPException(status_code=500, detail=f"BigQuery insert failed: {errors}")

            from google.cloud import bigquery  # type: ignore
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
                    bigquery.ScalarQueryParameter("ts", "TIMESTAMP", datetime.datetime.now(datetime.timezone.utc)),
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
                    bigquery.ScalarQueryParameter("ts", "TIMESTAMP", datetime.datetime.now(datetime.timezone.utc)),
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
                snap = dict(_STATE)
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

    model_fqn = _bq_model_fqn()
    from google.cloud import bigquery  # type: ignore

    sql = f"""
      SELECT * FROM ML.PREDICT(MODEL `{model_fqn}`,
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

@app.get("/metrics")
def metrics():
    if USE_MOCK:
        with _state_lock:
            s = dict(_STATE)
            s.pop("sp", None)
    else:
        s = {}
    cfg = get_config()
    return {
        "version": SERVICE_VERSION,
        "mock": bool(USE_MOCK),
        "tick_sec": MOCK_TICK_SEC,
        "apply_enabled": bool(APPLY_ENABLED),
        "snapshot": s,
        "levers": list(cfg.get("levers", {}).keys()),
    }
