import sys
import logging
from typing import Tuple
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import get_db_engine, init_dq_log_table
from extract import extract_all_weather
from validate import validate_weather_records, log_invalid_records_to_db
from transform import transform_weather_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def init_warehouse_schema(engine: Engine) -> None:
    """
    Initializes the Kimball Star Schema tables and constraints in PostgreSQL:
      - dim_city (Primary Key: city_name)
      - dim_date (Primary Key: date_id)
      - fact_weather (Foreign Keys referencing dim_city and dim_date)
    """
    ddl_statements = """
    -- 1. City Dimension
    CREATE TABLE IF NOT EXISTS dim_city (
    -- From load.py:
        city_name VARCHAR(100) NOT NULL REFERENCES dim_city(city_name)
        country VARCHAR(10) NOT NULL,
        latitude NUMERIC(8, 5),
        longitude NUMERIC(8, 5)
    );

    -- 2. Date Dimension
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INT PRIMARY KEY,
        full_date DATE NOT NULL,
        day INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        day_of_week VARCHAR(15) NOT NULL,
        is_weekend BOOLEAN NOT NULL
    );

    -- 3. Weather Fact Table
    CREATE TABLE IF NOT EXISTS fact_weather (
        fact_id BIGSERIAL PRIMARY KEY,
        city_name VARCHAR(100) NOT NULL REFERENCES dim_city(city_name),
        date_id INT NOT NULL REFERENCES dim_date(date_id),
        temperature NUMERIC(5, 2),
        feels_like NUMERIC(5, 2),
        temp_min NUMERIC(5, 2),
        temp_max NUMERIC(5, 2),
        pressure INT,
        humidity INT,
        wind_speed NUMERIC(5, 2),
        wind_deg INT,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl_statements))
    logging.info("Star schema tables (dim_city, dim_date, fact_weather) initialized.")


def load_dimensions(
    df_dim_city: pd.DataFrame,
    df_dim_date: pd.DataFrame,
    engine: Engine
) -> Tuple[int, int]:
    """
    Loads dimension data using idempotent UPSERT / IGNORE pattern.
    Avoids duplicate key violations on repeated pipeline runs.
    """
    city_count = 0
    date_count = 0

    with engine.begin() as conn:
        # Load dim_city
        for _, row in df_dim_city.iterrows():
            stmt = text("""
                INSERT INTO dim_city (city_name, country, latitude, longitude)
                VALUES (:city_name, :country, :latitude, :longitude)
                ON CONFLICT (city_name) DO UPDATE SET
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude;
            """)
            conn.execute(stmt, row.to_dict())
            city_count += 1

        # Load dim_date
        for _, row in df_dim_date.iterrows():
            stmt = text("""
                INSERT INTO dim_date (date_id, full_date, day, month, year, day_of_week, is_weekend)
                VALUES (:date_id, :full_date, :day, :month, :year, :day_of_week, :is_weekend)
                ON CONFLICT (date_id) DO NOTHING;
            """)
            conn.execute(stmt, row.to_dict())
            date_count += 1

    logging.info(f"Loaded {city_count} records into dim_city, {date_count} records into dim_date.")
    return city_count, date_count


def load_facts(df_fact_weather: pd.DataFrame, engine: Engine) -> int:
    """
    Appends observation metrics into fact_weather.
    """
    if df_fact_weather.empty:
        logging.warning("fact_weather DataFrame is empty. No facts loaded.")
        return 0

    df_fact_weather.to_sql(
        name="fact_weather",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )
    logging.info(f"Successfully loaded {len(df_fact_weather)} rows into fact_weather.")
    return len(df_fact_weather)


def run_full_etl_pipeline() -> None:
    """
    Executes the end-to-end local ETL pipeline:
      Extract -> Validate -> Transform -> Load
    """
    print("=" * 60)
    print("STARTING COMPLETE WEATHER ETL PIPELINE RUN")
    print("=" * 60)

    engine = get_db_engine()

    # Ensure warehouse schema exists
    init_dq_log_table()
    init_warehouse_schema(engine)

    # 1. Extraction
    print("\n[STAGE 1: EXTRACT]")
    raw_records = extract_all_weather()
    if not raw_records:
        logging.error("ETL Aborted: Extraction stage yielded 0 records.")
        sys.exit(1)

    # 2. Validation
    print("\n[STAGE 2: VALIDATE]")
    valid_records, invalid_records = validate_weather_records(raw_records)
    if invalid_records:
        log_invalid_records_to_db(invalid_records, pipeline_run_id="etl_full_run")

    if not valid_records:
        logging.error("ETL Aborted: 0 valid records passed quality checks.")
        sys.exit(1)

    # 3. Transformation
    print("\n[STAGE 3: TRANSFORM]")
    df_dim_city, df_dim_date, df_fact_weather = transform_weather_data(valid_records)

    # 4. Loading
    print("\n[STAGE 4: LOAD]")
    load_dimensions(df_dim_city, df_dim_date, engine)
    loaded_facts = load_facts(df_fact_weather, engine)

    print("\n" + "=" * 60)
    print(f"[PIPELINE SUCCESS] {loaded_facts} weather facts loaded into warehouse.")
    print("=" * 60)


if __name__ == "__main__":
    run_full_etl_pipeline()
