import sys
import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple
import pandas as pd

from extract import extract_all_weather
from validate import validate_weather_records

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def transform_weather_data(
    clean_records: List[Dict[str, Any]]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Transforms clean in-memory records into three distinct dimensional DataFrames:
      1. df_dim_city
      2. df_dim_date
      3. df_fact_weather
    """
    if not clean_records:
        logging.warning("No clean records provided for transformation.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_raw = pd.DataFrame(clean_records)

    # -------------------------------------------------------------
    # 1. Transform Dimension: dim_city
    # -------------------------------------------------------------
    df_dim_city = df_raw[
        ["city_name", "country", "latitude", "longitude"]
    ].drop_duplicates(subset=["city_name"]).reset_index(drop=True)

    # -------------------------------------------------------------
    # 2. Transform Dimension: dim_date
    # -------------------------------------------------------------
    date_records = []
    for dt_str in df_raw["observed_at"].dropna().unique():
        dt_obj = datetime.fromisoformat(dt_str)
        date_records.append({
            "date_id": int(dt_obj.strftime("%Y%m%d")),
            "full_date": dt_obj.date(),
            "day": dt_obj.day,
            "month": dt_obj.month,
            "year": dt_obj.year,
            "day_of_week": dt_obj.strftime("%A"),
            "is_weekend": dt_obj.weekday() >= 5
        })

    df_dim_date = pd.DataFrame(date_records).drop_duplicates(subset=["date_id"]).reset_index(drop=True)

    # -------------------------------------------------------------
    # 3. Transform Fact: fact_weather
    # -------------------------------------------------------------
    df_fact_weather = df_raw[[
        "city_name",
        "date_id",
        "temperature",
        "feels_like",
        "temp_min",
        "temp_max",
        "pressure",
        "humidity",
        "wind_speed",
        "wind_deg"
    ]].copy()

    # Ensure clean numeric data types
    numeric_cols = [
        "temperature", "feels_like", "temp_min", "temp_max",
        "pressure", "humidity", "wind_speed", "wind_deg"
    ]
    for col in numeric_cols:
        df_fact_weather[col] = pd.to_numeric(df_fact_weather[col], errors="coerce")

    logging.info(
        f"Transformation complete: "
        f"Cities={len(df_dim_city)}, Dates={len(df_dim_date)}, Facts={len(df_fact_weather)}"
    )

    return df_dim_city, df_dim_date, df_fact_weather


if __name__ == "__main__":
    print("Testing Transformation Module (transform.py)...")

    # 1. Extract
    extracted = extract_all_weather()

    # 2. Validate
    valid, invalid = validate_weather_records(extracted)

    # 3. Transform
    dim_city, dim_date, fact_weather = transform_weather_data(valid)

    print("\n--- Transformed Data Preview ---")
    print("\n[dim_city]:")
    print(dim_city.head())

    print("\n[dim_date]:")
    print(dim_date.head())

    print("\n[fact_weather]:")
    print(fact_weather.head())

    print("\n[SUCCESS] Milestone 4 transformation logic verified.")
