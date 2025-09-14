# Placeholder predictor (replace with real LightGBM/XGBoost models)
# Keep the API: .predict(features: dict, action_delta: dict|None) -> dict
class DummyPredictor:
    def predict(self, features, action_delta=None):
        base = float(features.get("specific_power_kwh_per_ton", 12.0))
        # A tiny optimistic improvement to show the loop works
        return {"specific_power_kwh_per_ton": round(base * 0.99, 3)}
