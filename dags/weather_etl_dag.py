import os
import sys
from datetime import datetime, timedelta

# Resolve real path past any symlinks to locate proETL project root
DAG_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DAG_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from config import get_db_engine, init_dq_log_table
from extract import extract_all_weather
from validate import validate_weather_records, log_invalid_records_to_db
from transform import transform_weather_data
from load import init_warehouse_schema, load_dimensions, load_facts


default_args = {
    "owner": "voidhyr",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def task_extract(**context):
    records = extract_all_weather()
    if not records:
        raise ValueError("Extraction returned 0 records. Halting pipeline.")
    return records


def task_validate(**context):
    ti = context["ti"]
    raw_records = ti.xcom_pull(task_ids="extract_weather_data")

    init_dq_log_table()
    valid_records, invalid_records = validate_weather_records(raw_records)

    if invalid_records:
        run_id = context.get("run_id", "manual_run")
        log_invalid_records_to_db(invalid_records, pipeline_run_id=run_id)

    if not valid_records:
        raise ValueError("All records failed data quality checks. Halting pipeline.")

    return valid_records


def task_transform(**context):
    ti = context["ti"]
    valid_records = ti.xcom_pull(task_ids="validate_weather_data")

    dim_city, dim_date, fact_weather = transform_weather_data(valid_records)

    return {
        "dim_city": dim_city.to_dict(orient="records"),
        "dim_date": dim_date.to_dict(orient="records"),
        "fact_weather": fact_weather.to_dict(orient="records"),
    }


def task_load(**context):
    ti = context["ti"]
    transformed = ti.xcom_pull(task_ids="transform_weather_data")

    df_dim_city = pd.DataFrame(transformed["dim_city"])
    df_dim_date = pd.DataFrame(transformed["dim_date"])
    df_fact_weather = pd.DataFrame(transformed["fact_weather"])

    engine = get_db_engine()
    init_warehouse_schema(engine)
    load_dimensions(df_dim_city, df_dim_date, engine)
    loaded_facts = load_facts(df_fact_weather, engine)

    print(f"Loaded {loaded_facts} fact records into PostgreSQL warehouse.")


with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="Automated Weather ETL Pipeline with 4-Pillar Data Quality Validation",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["etl", "weather", "msc_project", "data_quality"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=task_extract,
    )

    validate_task = PythonOperator(
        task_id="validate_weather_data",
        python_callable=task_validate,
    )

    transform_task = PythonOperator(
        task_id="transform_weather_data",
        python_callable=task_transform,
    )

    load_task = PythonOperator(
        task_id="load_weather_warehouse",
        python_callable=task_load,
    )

    extract_task >> validate_task >> transform_task >> load_task  # pyright: ignore[reportUnusedExpression]
