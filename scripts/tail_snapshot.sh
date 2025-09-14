#!/usr/bin/env bash
while true; do
  curl -s http://127.0.0.1:8000/snapshot \
  | jq '{production_tph, kiln_feed_tph, separator_dp_pa, id_fan_flow_Nm3_h, cooler_airflow_Nm3_h, kiln_speed_rpm, o2_percent, specific_power_kwh_per_ton}'
  sleep 5
done
