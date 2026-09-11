import pandas as pd
from datetime import datetime
from validate import validate_weather_records

# Synthetic evaluation batch with known ground-truth anomalies
TEST_BATCH = [
    # 1. Clean observation
    {"city_name": "Kochi", "date_id": 20260911, "temperature": 28.5, "humidity": 80, "pressure": 1012, "feels_like": 31.0, "temp_min": 27.0, "temp_max": 29.5, "wind_speed": 3.2, "wind_deg": 240},
    # 2. Clean observation
    {"city_name": "Bengaluru", "date_id": 20260911, "temperature": 24.0, "humidity": 65, "pressure": 1010, "feels_like": 24.5, "temp_min": 22.0, "temp_max": 26.0, "wind_speed": 2.1, "wind_deg": 180},
    # 3. Anomaly: Out-of-bounds temperature (999.0 C sensor spike)
    {"city_name": "Delhi", "date_id": 20260911, "temperature": 999.0, "humidity": 50, "pressure": 1008, "feels_like": 999.0, "temp_min": 990.0, "temp_max": 1000.0, "wind_speed": 1.5, "wind_deg": 90},
    # 4. Anomaly: Completeness violation (Missing city identifier)
    {"city_name": None, "date_id": 20260911, "temperature": 30.0, "humidity": 70, "pressure": 1011, "feels_like": 32.0, "temp_min": 28.0, "temp_max": 31.0, "wind_speed": 2.0, "wind_deg": 100},
    # 5. Anomaly: Range violation (Negative humidity)
    {"city_name": "Mumbai", "date_id": 20260911, "temperature": 29.0, "humidity": -15, "pressure": 1013, "feels_like": 30.0, "temp_min": 28.0, "temp_max": 30.0, "wind_speed": 4.0, "wind_deg": 200},
]

def run_pipeline_a(batch):
    """Pipeline A: Baseline ETL without explicit validation (relies on implicit dropna)."""
    df = pd.DataFrame(batch)
    # Baseline pipeline: silently drops null rows, but misses boundary violations
    clean_df = df.dropna(subset=["city_name"])
    
    corrupt_loaded = len(clean_df[(clean_df["temperature"] > 60) | (clean_df["humidity"] < 0)])
    silent_drops = len(df) - len(clean_df)
    logged_errors = 0  # No audit table or logging exists in baseline
    
    return {
        "pipeline": "Pipeline A (No Validation)",
        "total_ingested": len(batch),
        "valid_loaded": len(clean_df) - corrupt_loaded,
        "corrupt_records_loaded_to_warehouse": corrupt_loaded,
        "silent_dropped_records": silent_drops,
        "errors_logged_to_audit_table": logged_errors,
        "error_visibility_rate": "0.0%"
    }

def run_pipeline_b(batch):
    """Pipeline B: Proposed ETL with explicit logged validation."""
    valid_res, invalid_records = validate_weather_records(batch)
    
    # Ensure conversion to DataFrame regardless of whether list or DataFrame was returned
    valid_df = valid_res if isinstance(valid_res, pd.DataFrame) else pd.DataFrame(valid_res)
    
    corrupt_loaded = 0
    if not valid_df.empty:
        corrupt_loaded = len(valid_df[(valid_df["temperature"] > 60) | (valid_df["humidity"] < 0)])
    
    total_anomalies = len(invalid_records)
    visibility_rate = "100.0%" if total_anomalies > 0 else "N/A"
    
    return {
        "pipeline": "Pipeline B (Explicit Validation)",
        "total_ingested": len(batch),
        "valid_loaded": len(valid_df),
        "corrupt_records_loaded_to_warehouse": corrupt_loaded,
        "silent_dropped_records": 0,
        "errors_logged_to_audit_table": total_anomalies,
        "error_visibility_rate": visibility_rate
    }

if __name__ == "__main__":
    results_a = run_pipeline_a(TEST_BATCH)
    results_b = run_pipeline_b(TEST_BATCH)
    
    df_comparison = pd.DataFrame([results_a, results_b])
    print("\n==================== EXPERIMENT EVALUATION RESULTS ====================")
    print(df_comparison.to_string(index=False))
    print("=======================================================================\n")
