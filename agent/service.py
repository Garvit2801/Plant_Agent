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
                "O2 in [2.8, 4.5] %",
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

# -------------------------
# App & CORS
# -------------------------
app = FastAPI(title="Plant Agent API", version=SERVICE_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # tighten to your domains in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Robust plant.yaml resolver
# -------------------------
def _resolve_config_path() -> str:
    """Find plant.yaml via env or common locations and log helpful context."""
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

# Config cache keyed by resolved path
_config_cache: Dict[str, Any] = {}
_config_mtime: Optional[float] = None
_config_path: Optional[str] = None

def _stat_mtime(path: str) -> Optional[float]:
    try:
        return os.path.getmtime(path)
    except FileNotFoundError:
        return None

def get_config() -> Dict[str, Any]:
    """Load YAML once; auto-reload if the file changes."""
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
    """Light-weight placeholder: pretend a small improvement."""
    cur = float(snapshot.get("specific_power_kwh_per_ton", 12.5))
    return round(cur * 0.99, 3)

# -------------------------
# Mock plant state & thread
# -------------------------
_state_lock = threading.Lock()
_STATE: Dict[str, float] = {
    "production_tph": 10.0,
    "kiln_feed_tph": 10.0,
    "separator_dp_pa": 620.0,
    "id_fan_flow_Nm3_h": 150000.0,
    "cooler_airflow_Nm3_h": 220000.0,
    "kiln_speed_rpm": 3.5,
    "o2_percent": 3.3,
    "specific_power_kwh_per_ton": 12.5,
}

def _physics_step(state: Dict[str, float]) -> None:
    # O2 varies with ID fan flow (toy model)
    idflow = state["id_fan_flow_Nm3_h"]
    o2 = 2.5 + (idflow - 140000.0) / 100000.0
    state["o2_percent"] = clamp(o2, 2.2, 5.0)

    # Specific power weakly depends on flows and dp (toy model)
    base = 12.0 + (state["separator_dp_pa"] - 600.0) / 4000.0
    base += (state["id_fan_flow_Nm3_h"] - 150000.0) / 600000.0
    base += (state["cooler_airflow_Nm3_h"] - 220000.0) / 800000.0
    state["specific_power_kwh_per_ton"] = round(base, 3)

def _mock_loop():
    while True:
        time.sleep(MOCK_TICK_SEC)
        with _state_lock:
            _physics_step(_STATE)

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
    setpoints: Dict[str, float]

class SnapshotSetReq(BaseModel):
    setpoints: Dict[str, float]

# -------------------------
# BigQuery client (optional at runtime)
# -------------------------
_BQ_ENABLED = False
_BQ_ERR: Optional[str] = None
try:
    from google.cloud import bigquery  # type: ignore
    _bq_client = bigquery.Client()  # project auto-detected on Cloud Run
    _BQ_ENABLED = True
except Exception as e:
    _bq_client = None
    _BQ_ENABLED = False
    _BQ_ERR = f"BigQuery client not initialized: {e}"

def _bq_table_path() -> str:
    # Allow override via env; else default to <project>.plant_ops.snapshots
    tbl = os.getenv("BQ_SNAPSHOTS_TABLE")
    if tbl:
        return tbl
    if not PROJECT_ID:
        raise HTTPException(status_code=500, detail="PROJECT_ID not found for BigQuery table")
    return f"{PROJECT_ID}.plant_ops.snapshots"

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

# NEW: friendly alias + HEAD support
@app.get("/health")
def health():
    return healthz()

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
    """Helps verify resolution in Cloud Run."""
    known = ["/app/config/plant.yaml", "/app/plant.yaml", "config/plant.yaml", "plant.yaml"]
    return {
        "PLANT_CONFIG_env": os.getenv("PLANT_CONFIG"),
        "known_locations": known,
        "exists": {p: os.path.exists(p) for p in known},
        "resolved_path": _resolve_config_path() if os.getenv("PLANT_CONFIG") or any(os.path.exists(p) for p in known) else None,
        "keys": list(get_config().keys()),
        "bq_enabled": _BQ_ENABLED,
        "bq_error": _BQ_ERR,
        "bq_table": os.getenv("BQ_SNAPSHOTS_TABLE", f"{PROJECT_ID}.plant_ops.snapshots" if PROJECT_ID else None),
        "project_id": PROJECT_ID or None,
    }

@app.get("/snapshot")
def snapshot():
    if USE_MOCK:
        with _state_lock:
            return dict(_STATE)
    raise HTTPException(status_code=501, detail="Live plant connectors not configured")

@app.post("/snapshot/set")
def snapshot_set(req: SnapshotSetReq):
    """Manually nudge mock values (testing dashboards/flows)."""
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

    # Build supports-only energy trims (respect hold_in_routine in config)
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
def actuate_apply_stage(req: ApplyStageReq):
    if not APPLY_ENABLED:
        return {"ok": True, "note": "APPLY_ENABLED=0; dry-run"}

    setpts = req.setpoints or {}
    if USE_MOCK:
        with _state_lock:
            for k, v in setpts.items():
                if k not in _STATE:
                    continue
                cur = float(_STATE[k])
                tgt = float(v)
                nxt = cur + (tgt - cur) * 0.9
                _STATE[k] = round(nxt, 12)
            _physics_step(_STATE)
        return {"ok": True}

    # TODO: wire to real plant (OPC UA / Modbus)
    return {"ok": True, "note": "Live plant write path not implemented"}

@app.post("/actuate/rollback")
def actuate_rollback():
    if USE_MOCK:
        return {"ok": True, "note": "mock: nothing to rollback"}
    return {"ok": True, "note": "Live plant rollback not implemented"}

# -------------------------
# NEW: /ingest → BigQuery
# -------------------------
@app.post("/ingest")
def ingest(doc: dict = Body(default={})):
    """
    Upserts a single row into BigQuery table (default: <project>.plant_ops.snapshots).
    Body may contain {"snapshot": {...}}; if absent, the current /snapshot is fetched.
    """
    if not _BQ_ENABLED or _bq_client is None:
        raise HTTPException(status_code=500, detail=_BQ_ERR or "BigQuery unavailable")

    try:
        snap = doc.get("snapshot") or snapshot()
        # basic presence check
        required = [
            "production_tph", "kiln_feed_tph", "separator_dp_pa",
            "id_fan_flow_Nm3_h", "cooler_airflow_Nm3_h",
            "kiln_speed_rpm", "o2_percent", "specific_power_kwh_per_ton",
        ]
        for k in required:
            if k not in snap:
                raise HTTPException(status_code=400, detail=f"snapshot missing field: {k}")

        row = {
            "ts": pd.Timestamp.utcnow().isoformat(),
            "source": doc.get("source", "scheduler"),
            "production_tph": float(snap["production_tph"]),
            "kiln_feed_tph": float(snap["kiln_feed_tph"]),
            "separator_dp_pa": float(snap["separator_dp_pa"]),
            "id_fan_flow_Nm3_h": float(snap["id_fan_flow_Nm3_h"]),
            "cooler_airflow_Nm3_h": float(snap["cooler_airflow_Nm3_h"]),
            "kiln_speed_rpm": float(snap["kiln_speed_rpm"]),
            "o2_percent": float(snap["o2_percent"]),
            "specific_power_kwh_per_ton": float(snap["specific_power_kwh_per_ton"]),
            "raw": snap,  # JSON column in BQ
        }

        table = _bq_table_path()
        errors = _bq_client.insert_rows_json(table, [row])  # type: ignore
        if errors:
            # errors is a list of row errors; surface the first one
            raise HTTPException(status_code=500, detail=f"BigQuery insert failed: {errors}")
        return {"ok": True, "table": table}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"/ingest error: {e}")

@app.get("/metrics")
def metrics():
    """Tiny JSON metrics for dashboards (Grafana/Cloud Run custom)."""
    if USE_MOCK:
        with _state_lock:
            s = dict(_STATE)
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
