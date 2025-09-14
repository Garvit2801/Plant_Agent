#!/usr/bin/env python3
import time, requests, os, json, sys, argparse

API = os.environ.get("AGENT_ENDPOINT", "http://127.0.0.1:8000")
HOLD_MIN = int(os.environ.get("HOLD_MIN", "10"))
MIN_REL_CHANGE = 0.01  # stop if all support trims <1%

def pct_change(old, new):
    try:
        return abs(new - old) / max(1e-9, abs(old))
    except Exception:
        return 0.0

def small_change(prev_set, new_set):
    keys = set(prev_set) | set(new_set)
    for k in keys:
        # Skip throughput levers in the small-change test
        if k in ("kiln_feed_tph", "kiln_speed_rpm"):
            continue
        if k in prev_set and k in new_set:
            if pct_change(prev_set[k], new_set[k]) >= MIN_REL_CHANGE:
                return False
    return True

def routine_trim(snapshot):
    r = requests.post(f"{API}/optimize/routine", json={"snapshot": snapshot}, timeout=15)
    r.raise_for_status()
    out = r.json()
    return out.get("proposed_setpoints", {}), out

def apply(setpoints, current=None):
    payload = {"setpoints": setpoints}
    if current:
        payload["current"] = current
    r = requests.post(f"{API}/actuate/apply_stage", json=payload, timeout=15)
    r.raise_for_status()
    return r.json().get("ok", False)

def main():
    p = argparse.ArgumentParser(description="Apply staged plan from /optimize/load or auto-trim via /optimize/routine.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--json", help="Plan JSON string (quote it!)")
    g.add_argument("--file", help="Path to JSON file containing the plan")
    g.add_argument("--stdin", action="store_true", help="Read JSON from stdin")
    p.add_argument("--hold-min", type=int, default=HOLD_MIN, help="Hold minutes between stages")
    p.add_argument("--mode", choices=["stages","routine"], default="stages",
                   help="'stages' to execute a /optimize/load plan; 'routine' to iteratively trim supports")
    args = p.parse_args()

    if args.stdin:
        plan_text = sys.stdin.read()
    elif args.file:
        with open(args.file, "r") as f:
            plan_text = f.read()
    else:
        plan_text = args.json

    try:
        plan = json.loads(plan_text)
    except json.JSONDecodeError as e:
        print("Invalid JSON input:", e)
        sys.exit(2)

    if args.mode == "stages":
        current = plan.get("current", {})
        stages = plan.get("stages", [])
        if not stages:
            print("No 'stages' found in the plan JSON.")
            sys.exit(1)
        for i, st in enumerate(stages, 1):
            print(f"Applying {st.get('name', f'Stage {i}')} → {st.get('setpoints')}")
            ok = apply(st.get("setpoints", {}), current if i == 1 and current else None)
            if not ok:
                print("Actuation failed, stopping.")
                sys.exit(1)
            # In production: read live snapshot & checks here; stop on any red
            for m in range(args.hold_min, 0, -1):
                print(f"  hold {m} min…", end="\r")
                time.sleep(60)
            print()
        print("All stages applied successfully.")
    else:
        # iterative routine trims until small changes
        snapshot = plan.get("current") or plan
        if not isinstance(snapshot, dict):
            print("Routine mode expects a snapshot dict in 'current' or top-level JSON.")
            sys.exit(1)
        last_set = {}
        step = 0
        while True:
            step += 1
            props, out = routine_trim(snapshot)
            if not props:
                print("No further trims proposed.")
                break
            print(f"[Step {step}] Proposed: {props}")
            if last_set and small_change(last_set, props):
                print("Changes are small; stopping.")
                break
            ok = apply(props, snapshot if step == 1 else None)
            if not ok:
                print("Actuation failed; stopping.")
                sys.exit(1)
            last_set = props
            # In a real plant, refresh snapshot from DCS; here, assume setpoints ≈ snapshot
            snapshot.update(props)
            for m in range(args.hold_min, 0, -1):
                print(f"  hold {m} min…", end="\r")
                time.sleep(60)
            print()
        print("Auto-trim completed.")

if __name__ == "__main__":
    main()
