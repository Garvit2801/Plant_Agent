# agent/mock_io.py
from __future__ import annotations
import time, math, threading
from typing import Dict, Any

# ---- Mock plant state (module-level singletons) ----
_STATE_LOCK = threading.RLock()

# Setpoints that the "DCS" is trying to hold
_setpoints: Dict[str, float] = {
    "kiln_feed_tph": 10.0,
    "separator_dp_pa": 620.0,
    "id_fan_flow_Nm3_h": 150000.0,
    "cooler_airflow_Nm3_h": 220000.0,
    "kiln_speed_rpm": 3.50,
}

# Live snapshot (what sensors read)
_snapshot: Dict[str, float] = {
    "production_tph": 10.0,
    "kiln_feed_tph": 10.0,
    "separator_dp_pa": 620.0,
    "id_fan_flow_Nm3_h": 150000.0,
    "cooler_airflow_Nm3_h": 220000.0,
    "kiln_speed_rpm": 3.50,
    "o2_percent": 3.3,
    "specific_power_kwh_per_ton": 12.5,
    "bagfilter_dp_pa_meas": 1400.0,
    "co_ppm": 60.0,
    "nox_mg_Nm3": 680.0,
    "stack_dust_mg_Nm3": 18.0,
}

# Internal clock
_last_ts = time.time()

def get_snapshot() -> Dict[str, Any]:
    with _STATE_LOCK:
        return dict(_snapshot)

def get_setpoints() -> Dict[str, Any]:
    with _STATE_LOCK:
        return dict(_setpoints)

def set_setpoints(new_sp: Dict[str, float]) -> None:
    with _STATE_LOCK:
        for k, v in new_sp.items():
            if k in _setpoints and isinstance(v, (int, float)):
                _setpoints[k] = float(v)

def _clip(x, lo, hi): 
    return max(lo, min(hi, x))

def _derive_o2(prod_tph: float, id_flow: float) -> float:
    # More ID flow -> higher O2; more production -> lower O2
    base = 3.2 + 0.000004 * (id_flow - 150000) - 0.08 * (prod_tph - 10.0)
    return _clip(base, 2.2, 5.0)

def _derive_power_kwhpt(prod_tph: float, sep_dp: float, id_flow: float, cool_flow: float) -> float:
    # Very rough: higher flows & DP cost power; some economies at higher tph.
    base = 12.0
    base += 0.00001 * (id_flow - 150000)       # +0.01 kWh/t per +1000 Nm3/h
    base += 0.000006 * (cool_flow - 220000)    # +0.006 per +1000 Nm3/h
    base += 0.0005  * (sep_dp - 620)           # +0.5 per +1000 Pa
    base -= 0.06    * (prod_tph - 10.0)        # modest economy at higher load
    return _clip(base, 9.0, 18.0)

def _first_order(now: float, target: float, tau_s: float, dt_s: float) -> float:
    # Simple first-order response toward target
    if tau_s <= 0: 
        return target
    alpha = 1.0 - math.exp(-dt_s / tau_s)
    return now + alpha * (target - now)

def step(dt_s: float = 5.0) -> None:
    """Advance the mock plant by dt_s seconds."""
    global _last_ts
    with _STATE_LOCK:
        # 1) main levers drift to their setpoints with distinct time constants
        _snapshot["kiln_feed_tph"]      = _first_order(_snapshot["kiln_feed_tph"],      _setpoints["kiln_feed_tph"],      tau_s=60,  dt_s=dt_s)
        _snapshot["separator_dp_pa"]    = _first_order(_snapshot["separator_dp_pa"],    _setpoints["separator_dp_pa"],    tau_s=45,  dt_s=dt_s)
        _snapshot["id_fan_flow_Nm3_h"]  = _first_order(_snapshot["id_fan_flow_Nm3_h"],  _setpoints["id_fan_flow_Nm3_h"],  tau_s=50,  dt_s=dt_s)
        _snapshot["cooler_airflow_Nm3_h"]= _first_order(_snapshot["cooler_airflow_Nm3_h"],_setpoints["cooler_airflow_Nm3_h"],tau_s=70, dt_s=dt_s)
        _snapshot["kiln_speed_rpm"]     = _first_order(_snapshot["kiln_speed_rpm"],     _setpoints["kiln_speed_rpm"],     tau_s=80,  dt_s=dt_s)

        # 2) production tracks kiln_feed with slight lag
        _snapshot["production_tph"] = _first_order(_snapshot["production_tph"], _snapshot["kiln_feed_tph"], tau_s=90, dt_s=dt_s)

        # 3) derived KPIs
        _snapshot["o2_percent"] = _derive_o2(_snapshot["production_tph"], _snapshot["id_fan_flow_Nm3_h"])
        _snapshot["specific_power_kwh_per_ton"] = _derive_power_kwhpt(
            _snapshot["production_tph"], _snapshot["separator_dp_pa"],
            _snapshot["id_fan_flow_Nm3_h"], _snapshot["cooler_airflow_Nm3_h"]
        )

        # 4) environment proxies vary mildly with flows
        _snapshot["bagfilter_dp_pa_meas"] = _clip(1200 + 0.3*(_snapshot["separator_dp_pa"]-600), 900, 1800)
        _snapshot["co_ppm"]  = _clip(40 + 0.00005*(200000 - _snapshot["id_fan_flow_Nm3_h"]), 10, 200)
        _snapshot["nox_mg_Nm3"] = _clip(650 + 0.0001*(_snapshot["burning_zone_temp_c"]-1400) if "burning_zone_temp_c" in _snapshot else 650, 400, 800)
        _snapshot["stack_dust_mg_Nm3"] = _clip(15 + 0.00001*( _snapshot["id_fan_flow_Nm3_h"] - 150000 ), 5, 30)

        _last_ts = time.time()

def background_tick(period_s: float = 5.0):
    """Start a background thread to tick the mock plant."""
    t = threading.Thread(target=_tick_loop, args=(period_s,), daemon=True)
    t.start()

def _tick_loop(period_s: float):
    while True:
        step(period_s)
        time.sleep(period_s)
