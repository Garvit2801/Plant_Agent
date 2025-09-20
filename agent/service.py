# agent/service.py
from __future__ import annotations

import os
import json
import logging
import glob
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import yaml
import pandas as pd
import datetime
from fastapi import FastAPI, HTTPException, Body
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

# Project (prefer explicit env)
PROJECT_ID = (
    os.getenv("PROJECT_ID")
    or os.getenv("GOOGLE_CLOUD_PROJECT")
    or os.getenv("GCP_PROJECT")
    or ""
)

BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-south2")
MODEL_NAME = os.getenv("BQ_MODEL_NAME", "spower_reg")

# Table envs (optional; if unset, we derive from project at runtime)
BQ_SNAPSHOTS_TABLE_ENV = os.getenv("BQ_SNAPSHOTS_TABLE")
BQ_PLANS_TABLE_ENV     = os.getenv("BQ_PLANS_TABLE")
BQ_ACTS_TABLE_ENV      = os.getenv("BQ_ACTUATIONS_TABLE")
BQ_ROUTINE_TABLE_ENV   = os.getenv("BQ_ROUTINE_TABLE")

# -------------------------
# App & CORS
# -------------------------
app = FastAPI(title="Plant Agent API", version=SERVICE_VERSION)

UI_ORIGINS = [
    "https://your-ui.example.com",
    # "http://localhost:3000",
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
    """Serialize dict/list/etc to a compact JSON literal string."""
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)

# -------------------------
# Mock plant state & thread (with SPs)
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
_STATE.setdefault("sp", {
    "kiln_feed_tph": _STATE["kiln_feed_tph"],
    "separator_dp_pa": _STATE["separator_dp_pa"],
    "id_fan_flow_Nm3_h": _STATE["id_fan_flow_Nm3_h"],
    "cooler_airflow_Nm3_h": _STATE["cooler_airflow_Nm3_h"],
    "kiln_speed_rpm": _STATE["kiln_speed_rpm"],
})

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
            state[k] += follow_alpha * (target - state[k])

    # 2) production_tph tracks kiln_feed SP
    tau_prod = 20.0
    prod_alpha = min(1.0, dt_sec / tau_prod)
    desired_prod = float(sp.get("kiln_feed_tph", state["kiln_feed_tph"])) * 1.00
    if not (2.5 <= state["o2_percent"] <= 4.5):
        desired_prod = min(desired_prod, state["production_tph"])
    prod_lo = levers.get("production_tph", {}).get("min", 0.0) or 0.0
    prod_hi = levers.get("production_tph", {}).get("max", 1e12)
    desired_prod = clamp(desired_prod, prod_lo, prod_hi)
    state["production_tph"] += prod_alpha * (desired_prod - state["production_tph"])

    # 3) O2 vs ID fan flow
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
    targets: Optional[Dict[str, Any]] = None
    constraints: Optional[Dict[str, Any]] = None

class LoadOptimizeReq(BaseModel):
    snapshot: Dict[str, Any]
    direction: str = Field(..., pattern="^(up|down)$")
    delta_pct: float = Field(..., gt=0, le=50)
    steps: Optional[int] = None

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

def _snapshots_table() -> str:
    return BQ_SNAPSHOTS_TABLE_ENV or f"{_effective_project()}.plant_ops.snapshots"

def _plans_table() -> str:
    return BQ_PLANS_TABLE_ENV or f"{_effective_project()}.plant_ops.plans_v2"

def _acts_table() -> str:
    return BQ_ACTS_TABLE_ENV or f"{_effective_project()}.plant_ops.actuations_v2"

def _routine_table() -> str:
    return BQ_ROUTINE_TABLE_ENV or f"{_effective_project()}.plant_ops.routine_suggestions_v2"

def _bq_table_path() -> str:  # snapshots legacy helper
    return _snapshots_table()

def _bq_model_fqn() -> str:
    return f"{_effective_project()}.plant_ops.{MODEL_NAME}"

def _latest_snapshot_from_bq() -> Dict[str, Any]:
    if not _BQ_ENABLED or _bq_client is None:
        return {}
    table = _snapshots_table()
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
            # ✅ BigQuery streaming expects JSON values as JSON-encoded strings
            if isinstance(value, (dict, list)):
                return json.dumps(value, separators=(",", ":"))
            if isinstance(value, (int, float, bool)) or value is None:
                return json.dumps(value)
            # assume it's already a JSON string
            return str(value)
        if field_type == "RECORD":
            # RECORD can take a dict; try to parse strings into dicts
            if isinstance(value, (dict, list)):
                return value
            try:
                parsed = json.loads(value)
                return parsed
            except Exception:
                # last resort: wrap as {"value": "..."} so it's still an object
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
        try:
            parsed = json.loads(json.dumps(value, default=str))
        except Exception:
            raise
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
            "/healthz", "/health", "/version", "/config",
            "/debug/config", "/debug/tables", "/debug/bq_recent",
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
    }

@app.get("/debug/tables")
def debug_tables():
    try:
        proj = _effective_project()
    except Exception as e:
        proj = f"(error: {e})"
    return {
        "effective_project": proj,
        "snapshots_table": _snapshots_table(),
        "plans_table": _plans_table(),
        "actuations_table": _acts_table(),
        "routine_table": _routine_table(),
        "bq_enabled": _BQ_ENABLED,
        "bq_location": BQ_LOCATION,
    }

@app.get("/debug/bq_recent")
def debug_bq_recent():
    return {
        "recent": list(reversed(_BQ_RECENT))[:10],
        "schemas": {
            "snapshots": _bq_get_schema(_snapshots_table()) if _BQ_ENABLED else {},
            "plans_v2": _bq_get_schema(_plans_table()) if _BQ_ENABLED else {},
            "actuations_v2": _bq_get_schema(_acts_table()) if _BQ_ENABLED else {},
            "routine_suggestions_v2": _bq_get_schema(_routine_table()) if _BQ_ENABLED else {},
        },
        "tables": {
            "snapshots": _snapshots_table(),
            "plans": _plans_table(),
            "acts": _acts_table(),
            "routine": _routine_table(),
        },
        "enabled": _BQ_ENABLED,
        "bq_error": _BQ_ERR,
    }

@app.get("/snapshot")
def snapshot():
    if USE_MOCK:
        with _state_lock:
            return {k: v for k, v in _STATE.items() if k != "sp"}
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
    return {"ok": True, "state": {k: v for k, v in _STATE.items() if k != "sp"}}

# -------------------------
# Optimize (Routine) + Suggestion logging
# -------------------------
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
        elif lever == "id_fan_flow_Nm3_h":
            base = s.get("id_fan_flow_Nm3_h", 150000) * 0.98
            recipe[lever] = max(meta.get("min", 0), min(meta.get("max", 1e12), base))
        elif lever == "cooler_airflow_Nm3_h":
            base = s.get("cooler_airflow_Nm3_h", 220000) * 0.98
            recipe[lever] = max(meta.get("min", 0), min(meta.get("max", 1e12), base))

    proposal_list = propose_actions(s, recipe, levers)
    proposal = proposal_list[0] if proposal_list else {}

    pred = {"specific_power_kwh_per_ton": predict_specific_power(s)}
    payload = {
        "mode": "routine",
        "current": s,
        "predicted_after": pred,
        "actions": {"apply_stage": True, "apply_all": False, "rollback": True},
        "proposed_setpoints": proposal,
        "match_info": {"candidates_used": 2008},
    }

    tbl = _routine_table()
    err = None
    if _BQ_ENABLED and tbl:
        # Coerce 'proposed_setpoints' to JSON string if the column is JSON
        schema = _bq_get_schema(tbl)
        proposed = proposal
        if schema.get("proposed_setpoints") == "JSON" and isinstance(proposal, (dict, list)):
            proposed = _as_json_string(proposal)

        suggestion_row = {
            "suggestion_id": str(uuid.uuid4()),
            "created_at": _now_ts(),
            "snapshot": s,
            "proposed_setpoints": proposed,
            "predicted_after": pred,
            "targets": req.targets,
            "constraints": req.constraints,
            "mode": "routine",
        }
        err = _bq_insert_flexible(tbl, suggestion_row)
        _remember_bq_attempt("routine_insert", tbl, list(suggestion_row.keys()), err)
        if err:
            logging.warning("routine_suggestions_v2 insert error: %s", err)
    else:
        logging.info("Skipping routine suggestion logging: BQ_ENABLED=%s, table=%s", _BQ_ENABLED, tbl)

    payload["bq_log"] = {"table": tbl, "insert_error": err}
    return payload

# -------------------------
# Optimize (Load Up/Down) + Plan Logging
# -------------------------
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

    stages = build_stage_plan(s, targets, levers, stages_max=req.steps or stages_max)
    pred = {"specific_power_kwh_per_ton": predict_specific_power(s)}

    plan_id = str(uuid.uuid4())
    created_at = _now_ts()
    payload = {
        "plan_id": plan_id,
        "created_at": created_at.isoformat(),
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

    tbl = _plans_table()
    err = None
    if _BQ_ENABLED and tbl:
        # Convert nested stages[].setpoints to JSON string (table defines it as JSON)
        stages_bq: List[Dict[str, Any]] = []
        for stg in stages:
            sp = stg.get("setpoints")
            sp_out = _as_json_string(sp) if isinstance(sp, (dict, list)) else sp
            stages_bq.append({
                "name": stg.get("name"),
                "setpoints": sp_out,
                "checks": stg.get("checks"),
            })

        plan_row = {
            "plan_id": plan_id,
            "created_at": created_at,
            "mode": f"load_{req.direction}",
            "direction": req.direction,
            "delta_pct": float(req.delta_pct),
            "steps": int(req.steps) if req.steps else None,
            "snapshot": s,
            "targets": targets,
            "stages": stages_bq,   # << important fix
            "predicted_after": pred,
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
def actuate_apply_stage(req: ApplyStageReq = Body(default={})):
    """
    Accepts:
      - {"setpoints": {...}} OR {"stage": {"setpoints": {...}}} OR {"proposal": {...}} OR {"proposed_setpoints": {...}}
      - Optional metadata: plan_id, mode, stage_index
    """
    if not APPLY_ENABLED:
        return {"ok": True, "note": "APPLY_ENABLED=0; dry-run"}

    setpts = req.extract_setpoints()
    if not isinstance(setpts, dict) or not setpts:
        raise HTTPException(status_code=422, detail="No setpoints provided")

    # capture before snapshot
    if USE_MOCK:
        with _state_lock:
            before = {k: v for k, v in _STATE.items() if k != "sp"}

    # apply to mock
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

            # immediate nudge
            for k in ("kiln_feed_tph","separator_dp_pa","id_fan_flow_Nm3_h","cooler_airflow_Nm3_h","kiln_speed_rpm"):
                if k in sp:
                    cur = float(_STATE[k])
                    tgt = float(sp[k])
                    _STATE[k] = cur + 0.5*(tgt - cur)

            _physics_tick(_STATE, dt_sec=MOCK_TICK_SEC)
            after = {k: v for k, v in _STATE.items() if k != "sp"}
    else:
        before = {}
        after = {}

    applied_at = _now_ts()

    # log to BQ
    tbl = _acts_table()
    err = None
    if _BQ_ENABLED and tbl:
        schema = _bq_get_schema(tbl)

        def _maybe_stringify(val: Any, col: str):
            # If the column is not a STRUCT/RECORD, stringify dicts/lists to a compact JSON string
            col_type = schema.get(col)
            if isinstance(val, (dict, list)) and col_type != "RECORD":
                return _as_json_string(val)
            return val
        # If 'setpoints' column is JSON, serialize dict to JSON string
        setpoints_for_bq: Any = setpts

        stage_name = (req.stage or {}).get("name") if isinstance(req.stage, dict) else None
        deltas_obj = _diff_kpis(before, after) if before and after else None
        before_for_bq = _maybe_stringify(before,  "before")
        after_for_bq  = _maybe_stringify(after,   "after")
        deltas_for_bq = _maybe_stringify(deltas_obj, "deltas")


        act_row = {
            "applied_at": _now_ts(),
            "mode": req.mode,
            "plan_id": req.plan_id,
            "stage_index": req.stage_index,
            "stage_name": stage_name,
            "setpoints": setpoints_for_bq,
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

    return {"ok": True, "bq_log": {"table": tbl, "insert_error": err}}

@app.post("/actuate/rollback")
def actuate_rollback():
    if USE_MOCK:
        return {"ok": True, "note": "mock: nothing to rollback"}
    return {"ok": True, "note": "Live plant rollback not implemented"}

# -------------------------
# /ingest → BigQuery (snapshots)
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

@app.get("/metrics")
def metrics():
    if USE_MOCK:
        with _state_lock:
            s = {k: v for k, v in _STATE.items() if k != "sp"}
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
