import time, json

def log_event(kind: str, payload: dict):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), kind, json.dumps(payload)[:1000])
