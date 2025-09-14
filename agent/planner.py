# agent/planner.py
from typing import Dict, Any, List
import math

def _pct_step_of_current(current: float, target: float, step_pct: float) -> float:
    """
    Move by step_pct of *current* toward target (no overshoot), respecting direction.
    """
    if current is None or target is None:
        return None
    step = abs(current) * (step_pct / 100.0)
    if step == 0:
        return target
    delta = target - current
    if abs(delta) <= step:
        return target
    return current + math.copysign(step, delta)

def _abs_step(current: float, target: float, step_abs: float) -> float:
    if current is None or target is None:
        return None
    delta = target - current
    if abs(delta) <= step_abs:
        return target
    return current + math.copysign(step_abs, delta)

def propose_actions(
    now: Dict[str, Any],
    recipe: Dict[str, Any],
    levers: Dict[str, Any],
    *,
    mode: str = "routine"
) -> List[Dict[str, Any]]:
    """
    Routine: one nudge toward recipe. Skips levers marked hold_in_routine.
    For load-up/down, pass mode="load".
    """
    merged = {}
    for lever, cfg in levers.items():
        if mode == "routine" and cfg.get("hold_in_routine"):
            continue
        if lever in now and lever in recipe:
            lo, hi = cfg.get("min", -1e9), cfg.get("max", 1e9)
            if "step_pct" in cfg:
                nxt = _pct_step_of_current(now[lever], recipe[lever], cfg["step_pct"])
            elif "step_abs" in cfg:
                nxt = _abs_step(now[lever], recipe[lever], cfg["step_abs"])
            else:
                continue
            if nxt is not None:
                merged[lever] = round(max(lo, min(hi, nxt)), 3)
    return [merged] if merged else []

def build_stage_plan(now: Dict[str, Any],
                     target: Dict[str, Any],
                     levers: Dict[str, Any],
                     stages_max: int = 4) -> List[Dict[str, Any]]:
    """
    Build a staged plan from 'now' to 'target', respecting step sizes.
    Returns a list: [{name, setpoints, checks}]
    """
    stages: List[Dict[str, Any]] = []
    current = dict(now)

    for s in range(1, stages_max + 1):
        stage_set = {}
        moved_any = False
        for lever, cfg in levers.items():
            if lever not in target or lever not in current:
                continue
            lo, hi = cfg.get("min", -1e9), cfg.get("max", 1e9)

            if "step_pct" in cfg:
                nxt = _pct_step_of_current(current[lever], target[lever], cfg["step_pct"])
            elif "step_abs" in cfg:
                nxt = _abs_step(current[lever], target[lever], cfg["step_abs"])
            else:
                continue

            if nxt is None:
                continue
            nxt = max(lo, min(hi, nxt))
            if abs(nxt - current[lever]) > 1e-6:
                stage_set[lever] = round(nxt, 3)
                moved_any = True

        if not moved_any:
            break

        stages.append({
            "name": f"Stage {s}",
            "setpoints": stage_set,
            "checks": [
                "O2 in [2.8, 4.5] %",
                "CO < 180 ppm",
                "Bagfilter ΔP < 1800 Pa",
                "Mill outlet temp 90–120 °C",
                "Drive load factor < 95%"
            ]
        })
        current.update(stage_set)

        # Stop early if close to target
        done = True
        for lever in stage_set.keys():
            tgt = target.get(lever)
            cur = current.get(lever)
            if tgt is None or cur is None:
                continue
            if abs(tgt - cur) > max(0.01 * abs(tgt), 1e-3):
                done = False
                break
        if done:
            break

    return stages

def build_ui_payload(mode: str, now: Dict[str, Any], proposal: Any, pred: Dict[str, Any]) -> Dict[str, Any]:
    """Uniform payload for routine (single-step) and load change (stages)."""
    payload = {
        "mode": mode,
        "current": now,
        "predicted_after": pred,
        "actions": {"apply_stage": True, "apply_all": mode.startswith("load_"), "rollback": True}
    }
    if isinstance(proposal, list):  # staged plan
        payload["stages"] = proposal
    else:
        payload["proposed_setpoints"] = proposal or {}
    return payload
