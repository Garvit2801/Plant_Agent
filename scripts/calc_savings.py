#!/usr/bin/env python3
"""
Compute kWh/t, hourly/day/month energy, cost, and CO2 savings.

Inputs:
- BEFORE snapshot JSON (e.g., before_trim.json)
- AFTER  snapshot JSON (e.g., curl /snapshot > after.json)
Optionally:
- Routine/plan JSON for predicted-after reference

Defaults:
- tariff: 7.0 (currency/kWh)           # change with --tariff
- co2_factor: 0.8 (kg CO2 per kWh)     # change with --co2
"""

import json, argparse, csv, time, os, sys
from typing import Dict, Any

def load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"ERROR: cannot read JSON from {path}: {e}", file=sys.stderr)
        sys.exit(2)

def num(d: Dict[str, Any], key: str, default=None, required=False):
    v = d.get(key, default)
    try:
        return float(v)
    except Exception:
        if required:
            print(f"ERROR: '{key}' missing or not numeric in snapshot.", file=sys.stderr)
            sys.exit(2)
        return default

def main():
    ap = argparse.ArgumentParser(description="Calculate savings from before/after plant snapshots.")
    ap.add_argument("--before", required=True, help="Path to BEFORE snapshot JSON")
    ap.add_argument("--after",  required=True, help="Path to AFTER snapshot JSON")
    ap.add_argument("--plan",   help="Optional plan/routine JSON (to include predicted-after)")
    ap.add_argument("--tariff", type=float, default=7.0, help="Electricity tariff (currency/kWh), default 7.0")
    ap.add_argument("--co2",    type=float, default=0.8, help="CO2 factor (kg CO2 per kWh), default 0.8")
    ap.add_argument("--hours",  type=float, default=24.0, help="Hours per day for daily savings, default 24")
    ap.add_argument("--days",   type=float, default=30.0, help="Days per month for monthly savings, default 30")
    ap.add_argument("--log",    default="data/run_log.csv", help="CSV log file to append a summary row")
    args = ap.parse_args()

    before = load_json(args.before)
    after  = load_json(args.after)
    plan   = load_json(args.plan) if args.plan else {}

    # Pull key metrics
    kwhpt_b = num(before, "specific_power_kwh_per_ton", required=True)
    kwhpt_a = num(after,  "specific_power_kwh_per_ton", required=True)

    # Use AFTER production for savings (what you’re actually running at)
    tph_a = num(after, "production_tph", required=True)

    # Compute energy per hour
    e_before_h = kwhpt_b * tph_a
    e_after_h  = kwhpt_a * tph_a
    e_sav_h    = e_before_h - e_after_h

    # Daily / monthly
    e_sav_day  = e_sav_h * args.hours
    e_sav_mon  = e_sav_day * args.days

    # Cost savings
    cost_h   = e_sav_h   * args.tariff
    cost_day = e_sav_day * args.tariff
    cost_mon = e_sav_mon * args.tariff

    # CO2 savings (kg); also show tonnes
    co2_h_kg   = e_sav_h   * args.co2
    co2_day_kg = e_sav_day * args.co2
    co2_mon_kg = e_sav_mon * args.co2

    # Optional: predicted-after (if provided)
    pred_after = plan.get("predicted_after", {})
    kwhpt_pred = pred_after.get("specific_power_kwh_per_ton")

    # Print a neat summary
    print("\n=== Savings Summary ===")
    print(f"Throughput (after)            : {tph_a:.3f} TPH")
    print(f"Specific Power Before         : {kwhpt_b:.3f} kWh/t")
    print(f"Specific Power After          : {kwhpt_a:.3f} kWh/t")
    if kwhpt_pred is not None:
        try:
            print(f"Specific Power Predicted-After: {float(kwhpt_pred):.3f} kWh/t")
        except Exception:
            pass
    print(f"Δ Specific Power              : {kwhpt_b - kwhpt_a:+.3f} kWh/t")
    print()
    print(f"Energy saving / hour          : {e_sav_h:+.3f} kWh/h")
    print(f"Energy saving / day           : {e_sav_day:+.1f} kWh/day  (hours/day={args.hours})")
    print(f"Energy saving / month         : {e_sav_mon:+.0f} kWh/month (days/month={args.days})")
    print()
    print(f"Tariff                        : {args.tariff:.3f} currency/kWh")
    print(f"Cost saving / hour            : {cost_h:+.2f}")
    print(f"Cost saving / day             : {cost_day:+.2f}")
    print(f"Cost saving / month           : {cost_mon:+.2f}")
    print()
    print(f"CO2 factor                    : {args.co2:.3f} kg/kWh")
    print(f"CO2 saving / hour             : {co2_h_kg:+.2f} kg  ({co2_h_kg/1000:+.3f} t)")
    print(f"CO2 saving / day              : {co2_day_kg:+.0f} kg ({co2_day_kg/1000:+.2f} t)")
    print(f"CO2 saving / month            : {co2_mon_kg:+.0f} kg ({co2_mon_kg/1000:+.1f} t)")
    print()

    # Append a CSV row for audit
    os.makedirs(os.path.dirname(args.log), exist_ok=True)
    with open(args.log, "a", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            time.strftime("%F %T"),
            tph_a,
            kwhpt_b, kwhpt_a, (kwhpt_b - kwhpt_a),
            e_sav_h, e_sav_day, e_sav_mon,
            args.tariff, cost_h, cost_day, cost_mon,
            args.co2, co2_h_kg, co2_day_kg, co2_mon_kg,
            os.path.basename(args.before),
            os.path.basename(args.after),
            os.path.basename(args.plan) if args.plan else ""
        ])
    print(f"Logged summary to {args.log}")

if __name__ == "__main__":
    main()
