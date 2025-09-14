import time, requests, os, json

SNAPSHOT = {
    "production_tph": 10.0, "kiln_feed_tph": 10.0, "separator_dp_pa": 620,
    "id_fan_flow_Nm3_h": 150000, "cooler_airflow_Nm3_h": 220000,
    "kiln_speed_rpm": 3.5, "o2_percent": 3.3, "specific_power_kwh_per_ton": 12.5
}

def main():
    endpoint = os.environ.get("AGENT_ENDPOINT", "http://127.0.0.1:8000/optimize/routine")
    while True:
        try:
            r = requests.post(endpoint, json={"snapshot": SNAPSHOT}, timeout=5)
            print("Decision:", json.dumps(r.json())[:500])
        except Exception as e:
            print("Scheduler error:", e)
        time.sleep(600)

if __name__ == "__main__":
    main()
