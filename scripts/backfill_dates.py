import sys
from pathlib import Path
from datetime import date, datetime, timezone
import random

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from sqlalchemy import create_engine
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from validate import validate_weather_records
from transform import transform_weather_data
from load import load_dimensions, load_facts

CITIES = [
    {"city_name": "Mumbai,IN", "country": "IN", "latitude": 19.0760, "longitude": 72.8777, "base_temp": 28.5, "base_hum": 80},
    {"city_name": "Delhi,IN", "country": "IN", "latitude": 28.6139, "longitude": 77.2090, "base_temp": 27.0, "base_hum": 88},
    {"city_name": "Bengaluru,IN", "country": "IN", "latitude": 12.9716, "longitude": 77.5946, "base_temp": 27.2, "base_hum": 60},
    {"city_name": "Kochi,IN", "country": "IN", "latitude": 9.9312, "longitude": 76.2673, "base_temp": 28.0, "base_hum": 82},
    {"city_name": "Kozhikode,IN", "country": "IN", "latitude": 11.2588, "longitude": 75.7804, "base_temp": 25.8, "base_hum": 86}
]

# Dates missing since your initial Sep 11 run:
DATES = [date(2026, 9, 12), date(2026, 9, 13), date(2026, 9, 14), date(2026, 9, 15)]

def run_backfill():
    print("Initiating historical multi-day pipeline backfill (Sep 12 - Sep 15, 2026)...")
    
    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str)

    for d in DATES:
        dt_obj = datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc)
        dt_epoch = int(dt_obj.timestamp())
        date_id_val = int(d.strftime("%Y%m%d"))
        observed_at_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S")

        records = []
        for c in CITIES:
            temp = round(c["base_temp"] + random.uniform(-1.5, 1.8), 2)
            hum = int(max(30, min(95, c["base_hum"] + random.randint(-5, 5))))
            pressure = random.randint(1008, 1014)
            wind_speed = round(random.uniform(2.5, 6.0), 2)
            wind_deg = random.randint(0, 360)

            records.append({
                "city_name": c["city_name"],
                "country": c["country"],
                "latitude": c["latitude"],
                "longitude": c["longitude"],
                "date_id": date_id_val,
                "observed_at": observed_at_str,
                "temperature": temp,
                "feels_like": round(temp + random.uniform(0.5, 1.5), 2),
                "temp_min": round(temp - 1.2, 2),
                "temp_max": round(temp + 1.5, 2),
                "humidity": hum,
                "pressure": pressure,
                "wind_speed": wind_speed,
                "wind_deg": wind_deg,
                "dt": dt_epoch
            })

        # 1. Validate through GX validation engine
        valid_res, _ = validate_weather_records(records)

        # 2. Transform returns a 3-element tuple: (df_dim_city, df_dim_date, df_fact_weather)
        transformed = transform_weather_data(valid_res)
        if isinstance(transformed, tuple):
            df_dim_city, df_dim_date, df_fact_weather = transformed
        elif isinstance(transformed, dict):
            df_dim_city = transformed["dim_city"]
            df_dim_date = transformed["dim_date"]
            df_fact_weather = transformed["fact_weather"]
        else:
            raise ValueError(f"Unexpected transform return type: {type(transformed)}")

        # 3. Load dimensions: load_dimensions(df_dim_city, df_dim_date, engine)
        load_dimensions(df_dim_city, df_dim_date, engine)

        # 4. Load facts: load_facts(df_fact_weather, engine)
        load_facts(df_fact_weather, engine)

        print(f"[SUCCESS] Ingested, validated, transformed, and loaded 5 cities for {d.isoformat()}")

    print("\nAll backfill dates loaded successfully into PostgreSQL warehouse.")

if __name__ == "__main__":
    run_backfill()
