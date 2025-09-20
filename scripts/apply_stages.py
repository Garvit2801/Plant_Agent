#!/usr/bin/env python3
"""
Robust stage applicator for Plant Agent.

Modes:
  - stages : consume a JSON plan (from --json/--file/--stdin) and apply its stages
  - routine: iterative support trims using /optimize/routine until changes are small
  - load   : build a plan with /optimize/load (e.g., --delta-pct 8 --direction up --steps 3) and apply

Auth:
  - Add OIDC token via --token or env AGENT_TOKEN. (Cloud Run private services)
Endpoint:
  - Set with --endpoint or env AGENT_ENDPOINT (defaults to http://127.0.0.1:8000)

Examples:
  # Apply a precomputed plan:
  python scripts/apply_stages.py --mode stages --file plan.json --hold-sec 8

  # Ask for +8% load-up and apply:
  python scripts/apply_stages.py --mode load --delta-pct 8 --direction up --steps 3 --hold-sec 8

  # Routine trims until changes <1% on support levers:
  python scripts/apply_stages.py --mode routine --hold-sec 8
"""
import argparse, json, os, sys, time, requests
from typing import Any, Dict, List, Optional, Tuple

# -----------------------
# Defaults & environment
# -----------------------
API_DEFAULT = os.environ.get("AGENT_ENDPOINT", "http://127.0.0.1:8000")
TOKEN_DEFAULT = os.environ.get("AGENT_TOKEN", "")
HOLD_MIN_DEFAULT = int(os.environ.get("HOLD_MIN", "10"))
MIN_REL_CHANGE = float(os.environ.get("MIN_REL_CHANGE", "0.01"))  # for routine trims

# -----------------------
# Helpers
# -----------------------
def pct_change(old: float, new: float) -> float:
    try:
        return abs(new - old) / max(1e-9, abs(old))
    except Exception:
        return 0.0

def small_change(prev_set: Dict[str, float], new_set: Dict[str, float]) -> bool:
    """True when all support trims (non-throughput levers) changed by < MIN_REL_CHANGE."""
    keys = set(prev_set) | set(new_set)
    for k in keys:
        if k in ("kiln_feed_tph", "kiln_speed_rpm"):  # throughput levers excluded from 'small change' check
            continue
        if k in prev_set and k in new_set and pct_change(prev_set[k], new_set[k]) >= MIN_REL_CHANGE:
            return False
    return True

def _headers(token: str = "") -> Dict[str, str]:
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

def _get(api: str, path: str, token: str, timeout=15) -> Dict[str, Any]:
    r = requests.get(f"{api}{path}", headers=_headers(token), timeout=timeout)
    _raise_for_status_verbose(r)
    return r.json()

def _post(api: str, path: str, payload: Dict[str, Any], token: str, timeout=20) -> Dict[str, Any]:
    r = requests.post(f"{api}{path}", headers=_headers(token), json=payload, timeout=timeout)
    _raise_for_status_verbose(r)
    return r.json()

def _raise_for_status_verbose(resp: requests.Response):
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        body = ""
        try:
            body = json.dumps(resp.json(), indent=2)
        except Exception:
            body = resp.text
        raise SystemExit(f"HTTP {resp.status_code} calling {resp.request.method} {resp.url}\n{body}") from e

def fetch_snapshot(api: str, token: str) -> Dict[str, Any]:
    return _get(api, "/snapshot", token)

def routine_trim(api: str, snapshot: Dict[str, Any], token: str) -> Tuple[Dict[str, float], Dict[str, Any]]:
    out = _post(api, "/optimize/routine", {"snapshot": snapshot}, token)
    # accept proposed_setpoints or setpoints/controls, normalize to setpoints
    ps = out.get("proposed_setpoints") or out.get("setpoints") or out.get("controls") or {}
    return ps, out

def load_plan(api: str, snapshot: Dict[str, Any], delta_pct: float, direction: str, steps: int, token: str) -> Dict[str, Any]:
    payload = {"snapshot": snapshot, "delta_pct": float(delta_pct), "direction": direction, "steps": int(steps)}
    return _post(api, "/optimize/load", payload, token)

def apply_setpoints(api: str, setpoints: Dict[str, float], token: str, current: Optional[Dict[str, Any]] = None) -> bool:
    """
    Try both accepted shapes:
      1) {"stage": {"setpoints": {...}}}
      2) {"setpoints": {...}}
    """
    body_stage = {"stage": {"setpoints": setpoints}}
    if current:
        body_stage["current"] = current
    try:
        out = _post(api, "/actuate/apply_stage", body_stage, token)
        return bool(out.get("ok"))
    except SystemExit:
        # fallback to plain setpoints
        body_plain = {"setpoints": setpoints}
        if current:
            body_plain["current"] = current
        out = _post(api, "/actuate/apply_stage", body_plain, token)
        return bool(out.get("ok"))

def plan_stages(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract stages list or wrap a single set of proposed setpoints as a stage."""
    stages = plan.get("stages") or plan.get("plan", {}).get("stages")
    if isinstance(stages, list) and stages:
        return stages
    # wrap single proposal
    sp = plan.get("proposed_setpoints") or plan.get("setpoints") or plan.get("controls")
    if isinstance(sp, dict) and sp:
        return [{"name": "Stage 1", "setpoints": sp}]
    return []

def stage_setpoints(stage: Dict[str, Any]) -> Dict[str, float]:
    if "setpoints" in stage and isinstance(stage["setpoints"], dict):
        return {k: float(v) for k, v in stage["setpoints"].items()}
    # tolerate alternate keys
    if "controls" in stage and isinstance(stage["controls"], dict):
        return {k: float(v) for k, v in stage["controls"].items()}
    if "proposed_setpoints" in stage and isinstance(stage["proposed_setpoints"], dict):
        return {k: float(v) for k, v in stage["proposed_setpoints"].items()}
    return {}

def hold_timer(seconds: int):
    for s in range(seconds, 0, -1):
        print(f"  hold {s}s…", end="\r", flush=True)
        time.sleep(1)
    print("  hold done".ljust(20))

# -----------------------
# Main
# -----------------------
def main():
    p = argparse.ArgumentParser(description="Apply staged plan, routine trims, or request load-up plan from Plant Agent.")
    p.add_argument("--endpoint", default=API_DEFAULT, help=f"Agent endpoint (default: {API_DEFAULT})")
    p.add_argument("--token", default=TOKEN_DEFAULT, help="OIDC token for private Cloud Run (or set AGENT_TOKEN)")
    p.add_argument("--hold-min", type=int, default=HOLD_MIN_DEFAULT, help="Hold minutes between stages (if --hold-sec not used)")
    p.add_argument("--hold-sec", type=int, default=None, help="Hold seconds between stages (overrides --hold-min)")
    p.add_argument("--mode", choices=["stages", "routine", "load"], default="stages",
                   help="stages: apply provided plan; routine: iterative trims; load: request plan with delta-pct")
    p.add_argument("--direction", choices=["up", "down"], default="up", help="For --mode load")
    p.add_argument("--delta-pct", type=float, default=None, help="Percent change for load mode, e.g., 8 for +8%%")
    p.add_argument("--steps", type=int, default=3, help="How many steps to request in load mode")
    # Plan input (for stages / routine). Optional in load mode.
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--json", help="Plan JSON string (quote it!) or a snapshot (for --mode routine)")
    g.add_argument("--file", help="Path to JSON file containing the plan (or snapshot for routine)")
    g.add_argument("--stdin", action="store_true", help="Read JSON from stdin")
    args = p.parse_args()

    api = args.endpoint.rstrip("/")
    token = args.token.strip()

    # Determine hold time
    hold_seconds = args.hold_sec if args.hold_sec is not None else args.hold_min * 60
    if hold_seconds <= 0:
        hold_seconds = 1

    # Get input JSON if provided
    plan_text = None
    if args.stdin:
        plan_text = sys.stdin.read()
    elif args.file:
        with open(args.file, "r") as f:
            plan_text = f.read()
    elif args.json:
        plan_text = args.json

    # Mode handlers
    if args.mode == "load":
        if args.delta_pct is None:
            sys.exit("For --mode load, please pass --delta-pct (e.g., 8 for +8%)")
        # 1) snapshot
        snap = fetch_snapshot(api, token)
        print("Snapshot:", json.dumps({k: snap.get(k) for k in ["production_tph","o2_percent","specific_power_kwh_per_ton"]}, indent=2))
        # 2) plan
        plan = load_plan(api, snap, args.delta_pct, args.direction, args.steps, token)
        stages = plan_stages(plan)
        if not stages:
            print("No stages returned by /optimize/load. Full response:")
            print(json.dumps(plan, indent=2))
            sys.exit(1)
        print(f"Plan has {len(stages)} stage(s). Target:", json.dumps(plan.get("target", {}), indent=2))
        current = plan.get("current") or snap
        # 3) apply
        for idx, st in enumerate(stages, 1):
            sps = stage_setpoints(st)
            print(f"\nApplying {st.get('name', f'Stage {idx}')} → {sps}")
            ok = apply_setpoints(api, sps, token, current if idx == 1 and current else None)
            if not ok:
                sys.exit("Actuation failed, stopping.")
            # observe
            hold_timer(hold_seconds)
            after = fetch_snapshot(api, token)
            print("Snapshot:", json.dumps({k: after.get(k) for k in ["production_tph","o2_percent","specific_power_kwh_per_ton"]}, indent=2))
        print("\nAll stages applied successfully.")
        return

    if args.mode == "stages":
        if not plan_text:
            sys.exit("Please provide a plan via --json/--file/--stdin")
        try:
            plan = json.loads(plan_text)
        except json.JSONDecodeError as e:
            sys.exit(f"Invalid JSON input: {e}")
        stages = plan_stages(plan)
        if not stages:
            sys.exit("No 'stages' found and no single proposal to apply.")
        current = plan.get("current", {})
        for i, st in enumerate(stages, 1):
            sps = stage_setpoints(st)
            print(f"Applying {st.get('name', f'Stage {i}')} → {sps}")
            ok = apply_setpoints(api, sps, token, current if i == 1 and current else None)
            if not ok:
                sys.exit("Actuation failed, stopping.")
            hold_timer(hold_seconds)
            after = fetch_snapshot(api, token)
            print("Snapshot:", json.dumps({k: after.get(k) for k in ["production_tph","o2_percent","specific_power_kwh_per_ton"]}, indent=2))
        print("All stages applied successfully.")
        return

    # args.mode == "routine"
    # If plan_text provided, treat it as a snapshot or wrapper with current
    snapshot = None
    if plan_text:
        try:
            maybe = json.loads(plan_text)
            snapshot = maybe.get("current") if isinstance(maybe, dict) and "current" in maybe else maybe
        except Exception:
            snapshot = None
    if not isinstance(snapshot, dict):
        snapshot = fetch_snapshot(api, token)
    print("Starting routine trims from snapshot:", json.dumps(snapshot, indent=2))
    last_set: Dict[str, float] = {}
    step = 0
    while True:
        step += 1
        props, out = routine_trim(api, snapshot, token)
        if not props:
            print("No further trims proposed.")
            break
        print(f"[Step {step}] Proposed: {props}")
        if last_set and small_change(last_set, props):
            print("Changes are small; stopping.")
            break
        ok = apply_setpoints(api, props, token, snapshot if step == 1 else None)
        if not ok:
            sys.exit("Actuation failed; stopping.")
        last_set = props
        # In a live plant, refresh from DCS; in mock, assume SP≈PV drifts toward setpoint
        hold_timer(hold_seconds)
        snapshot = fetch_snapshot(api, token)
        print("Snapshot:", json.dumps({k: snapshot.get(k) for k in ["production_tph","o2_percent","specific_power_kwh_per_ton"]}, indent=2))
    print("Auto-trim completed.")

if __name__ == "__main__":
    main()
