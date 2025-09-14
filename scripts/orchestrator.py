#!/usr/bin/env python3
"""
Automates:
  - routine trims on a schedule (e.g., every 60 min)
  - load change sequences on demand (reads a plan.json and executes stages)

Usage examples:
  # run routine loop forever (trim every 60 min, hold 10 min between micro-trims)
  python scripts/orchestrator.py routine --interval-min 60 --hold-min 10

  # execute a staged load plan once (generated from /optimize/load)
  python scripts/orchestrator.py load --plan plan.json --hold-min 10
"""
import os, time, json, argparse, sys
import requests
from typing import Dict, Any

API = os.environ.get("AGENT_ENDPOINT", "http://127.0.0.1:8000")
APPLY_ENABLED = os.environ.get("APPLY_ENABLED", "1") not in ("0","false","False")
SMALL_REL = float(os.environ.get("SMALL_REL", "0.01"))  # 1% change threshold

def get_snapshot() -> Dict[str,Any]:
    r = requests.get(f"{API}/snapshot", timeout=10); r.raise_for_status(); return r.json()

def optimize_routine(snapshot: Dict[str,Any]) -> Dict[str,Any]:
    r = requests.post(f"{API}/optimize/routine", json={"snapshot": snapshot}, timeout=20)
    r.raise_for_status(); return r.json()

def optimize_load(snapshot: Dict[str,Any], direction:str, delta_pct:float) -> Dict[str,Any]:
    r = requests.post(f"{API}/optimize/load",
                      json={"snapshot":snapshot,"direction":direction,"delta_pct":delta_pct},
                      timeout=30)
    r.raise_for_status(); return r.json()

def apply_stage(setpoints: Dict[str,float], current: Dict[str,Any] | None = None) -> bool:
    if not APPLY_ENABLED:
        print("[orchestrator] APPLY disabled (advisor mode). Would apply:", setpoints)
        return True
    payload = {"setpoints": setpoints}
    if current is not None: payload["current"] = current
    r = requests.post(f"{API}/actuate/apply_stage", json=payload, timeout=15)
    r.raise_for_status(); return bool(r.json().get("ok"))

def pct_change(old: float, new: float) -> float:
    denom = abs(old) if abs(old) > 1e-9 else 1.0
    return abs(new - old) / denom

def small_change(prev: Dict[str,float], new: Dict[str,float]) -> bool:
    keys = set(prev) | set(new)
    for k in keys:
        if k in ("kiln_feed_tph","kiln_speed_rpm"):  # throughput levers ignored in routine
            continue
        if k in prev and k in new and pct_change(prev[k], new[k]) >= SMALL_REL:
            return False
    return True

def routine_loop(interval_min: int, hold_min: int):
    """Every interval: read snapshot → ask routine → apply setpoints → hold → (optional micro-iterate)."""
    print(f"[orchestrator] routine loop started (interval={interval_min} min, hold={hold_min} min)")
    last_props: Dict[str,float] = {}
    while True:
        try:
            snap = get_snapshot()
            plan = optimize_routine(snap)
            props = plan.get("proposed_setpoints", {}) or {}
            if not props:
                print("[routine] no trims proposed.")
            else:
                if last_props and small_change(last_props, props):
                    print("[routine] trims are tiny; skipping this cycle.")
                else:
                    print(f"[routine] applying trims: {props}")
                    ok = apply_stage(props, current=snap)
                    if ok:
                        last_props = props
                        # simple hold
                        for m in range(hold_min, 0, -1):
                            print(f"  [hold] {m} min …", end="\r"); time.sleep(60)
                        print()
            # sleep until next interval
            for m in range(interval_min, 0, -1):
                print(f"[sleep] next routine in {m} min …", end="\r"); time.sleep(60)
            print()
        except KeyboardInterrupt:
            print("\n[orchestrator] stopping routine loop by user."); break
        except Exception as e:
            print(f"\n[routine][error] {e}")
            time.sleep(10)

def load_once(plan_path: str, hold_min: int):
    """Execute a staged load plan produced by /optimize/load."""
    with open(plan_path,"r") as f: plan = json.load(f)
    stages = plan.get("stages", [])
    current = plan.get("current", None)
    if not stages:
        print("[load] no stages found in plan."); sys.exit(2)
    for i, st in enumerate(stages, 1):
        name = st.get("name", f"Stage {i}")
        sps = st.get("setpoints", {})
        print(f"[load] applying {name}: {sps}")
        ok = apply_stage(sps, current if i == 1 else None)
        if not ok:
            print("[load] actuation failed; stopping."); sys.exit(1)
        for m in range(hold_min, 0, -1):
            print(f"  [hold] {m} min …", end="\r"); time.sleep(60)
        print()
    print("[load] all stages applied successfully.")

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("routine", help="Run routine trims forever on a cadence")
    r.add_argument("--interval-min", type=int, default=int(os.environ.get("ROUTINE_INTERVAL_MIN","60")))
    r.add_argument("--hold-min", type=int, default=int(os.environ.get("HOLD_MIN","10")))

    l = sub.add_parser("load", help="Execute a staged plan file from /optimize/load")
    l.add_argument("--plan", required=True, help="Path to plan.json")
    l.add_argument("--hold-min", type=int, default=int(os.environ.get("HOLD_MIN","10")))

    args = ap.parse_args()
    if args.cmd == "routine":
        routine_loop(args.interval_min, args.hold_min)
    else:
        load_once(args.plan, args.hold_min)

if __name__ == "__main__":
    main()
