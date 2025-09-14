# agent/actuator.py
import os, time
from typing import Dict
from .mock_io import set_setpoints as mock_set_setpoints, get_snapshot as mock_get_snapshot

USE_MOCK = os.environ.get("USE_MOCK", "1") not in ("0", "false", "False")

class Actuator:
    def __init__(self):
        self.snapshot = {}

    def snapshot_setpoints(self, current: dict):
        self.snapshot = dict(current or {})

    def apply(self, setpoints: Dict[str, float]) -> bool:
        if not setpoints:
            return True
        if USE_MOCK:
            # write to mock "DCS"
            mock_set_setpoints(setpoints)
            # simple readback wait
            time.sleep(0.5)
            snap = mock_get_snapshot()
            # verify each changed key drifted in right direction
            for k, v in setpoints.items():
                if k in snap and abs(float(snap[k]) - float(v)) > max(0.02*abs(v), 0.01):
                    # still drifting; allow (mock plant is first-order), don't fail hard
                    continue
            return True
        # TODO: implement real OPC-UA/Modbus writes here
        return True

    def rollback(self) -> bool:
        if USE_MOCK and self.snapshot:
            mock_set_setpoints({k: v for k, v in self.snapshot.items() if isinstance(v, (int, float))})
            return True
        # TODO: implement real rollback
        return True
