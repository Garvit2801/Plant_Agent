# agent/constraints.py
def would_violate(pred: dict, limits: dict) -> bool:
    """
    Returns True if any predicted KPI breaches configured hard limits.
    Add more nuanced logic as your models evolve.
    """
    for key, band in limits.items():
        if not isinstance(band, dict):
            continue
        if "min" in band and key in pred and pred[key] < band["min"]:
            return True
        if "max" in band and key in pred and pred[key] > band["max"]:
            return True
    return False
