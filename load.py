import logging
from sqlalchemy import text
import pandas as pd

logger = logging.getLogger(__name__)


def init_warehouse_schema(engine):
    """Initializes dim_city, dim_date, and fact_weather tables with valid constraints."""
    ddl = """
    CREATE TABLE IF NOT EXISTS dim_city (
        city_id SERIAL PRIMARY KEY,
        city_name VARCHAR(100) UNIQUE NOT NULL,
        country VARCHAR(10) NOT NULL,
        latitude NUMERIC(8, 5) NOT NULL,
        longitude NUMERIC(8, 5) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INT PRIMARY KEY,
        full_date DATE NOT NULL,
        day INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        day_of_week VARCHAR(15) NOT NULL,
        is_weekend BOOLEAN NOT NULL
    );

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
        aqi INT,
        pm2_5 NUMERIC(6, 2),
        pm10 NUMERIC(6, 2),
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Warehouse schema initialized successfully.")


def load_dimensions(df_dim_city: pd.DataFrame, df_dim_date: pd.DataFrame, engine):
    """Idempotently loads dimension records into PostgreSQL."""
    with engine.begin() as conn:
        for _, row in df_dim_city.iterrows():
            conn.execute(
                text("""
                INSERT INTO dim_city (city_name, country, latitude, longitude)
                VALUES (:city_name, :country, :latitude, :longitude)
                ON CONFLICT (city_name) DO UPDATE SET
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude;
                """),
                row.to_dict()
            )

        for _, row in df_dim_date.iterrows():
            conn.execute(
                text("""
                INSERT INTO dim_date (date_id, full_date, day, month, year, day_of_week, is_weekend)
                VALUES (:date_id, :full_date, :day, :month, :year, :day_of_week, :is_weekend)
                ON CONFLICT (date_id) DO NOTHING;
                """),
                row.to_dict()
            )
    logger.info("Dimensions successfully loaded.")


def load_facts(df_fact_weather: pd.DataFrame, engine):
    """Loads transformed fact records using native SQLAlchemy parameter binding."""
    insert_sql = text("""
        INSERT INTO fact_weather (
            city_name, date_id, temperature, feels_like, temp_min,
            temp_max, pressure, humidity, wind_speed, wind_deg,
            aqi, pm2_5, pm10
        ) VALUES (
            :city_name, :date_id, :temperature, :feels_like, :temp_min,
            :temp_max, :pressure, :humidity, :wind_speed, :wind_deg,
            :aqi, :pm2_5, :pm10
        )
    """)

    # Replace pandas NA / NaN with None so PostgreSQL receives proper SQL NULLs
    df_clean = df_fact_weather.where(pd.notnull(df_fact_weather), None)
    records = df_clean.to_dict(orient="records")

    if records:
        with engine.begin() as conn:
            conn.execute(insert_sql, records)
        logger.info(f"Loaded {len(records)} facts into fact_weather.")
    return len(records)