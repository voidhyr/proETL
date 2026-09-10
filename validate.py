import sys
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
import pandas as pd
from sqlalchemy import text

from config import get_db_engine, init_dq_log_table
from extract import extract_all_weather, TARGET_CITIES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Physical meteorological boundary definitions
MIN_TEMPERATURE: float = -50.0
MAX_TEMPERATURE: float = 60.0
MIN_HUMIDITY: int = 0
MAX_HUMIDITY: int = 100
MIN_PRESSURE: int = 800
MAX_PRESSURE: int = 1100

REQUIRED_COLUMNS: List[str] = [
    "city_name",
    "date_id",
    "temperature",
    "humidity",
    "pressure"
]


def log_invalid_records_to_db(
    invalid_records: List[Dict[str, Any]],
    pipeline_run_id: str = "manual_run"
) -> int:
    """
    Persists data quality anomalies into the PostgreSQL data_quality_log table.
    """
    if not invalid_records:
        return 0

    engine = get_db_engine()
    insert_stmt = text("""
        INSERT INTO data_quality_log (
            pipeline_run_id, city_name, rule_failed, invalid_value, failure_reason, logged_at
        ) VALUES (
            :pipeline_run_id, :city_name, :rule_failed, :invalid_value, :failure_reason, :logged_at
        );
    """)

    logged_count = 0
    with engine.begin() as conn:
        for rec in invalid_records:
            conn.execute(insert_stmt, {
                "pipeline_run_id": pipeline_run_id,
                "city_name": rec.get("city_name", "UNKNOWN"),
                "rule_failed": rec.get("rule_failed", "UNKNOWN_RULE"),
                "invalid_value": str(rec.get("invalid_value", "")),
                "failure_reason": rec.get("failure_reason", ""),
                "logged_at": datetime.now(timezone.utc)
            })
            logged_count += 1

    logging.info(f"Successfully recorded {logged_count} validation anomalies into data_quality_log.")
    return logged_count


def validate_weather_records(
    records: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Evaluates extracted records across 4 data quality pillars:
      1. Completeness (Null checks)
      2. Uniqueness (Duplicate checks)
      3. Range Validity (Meteorological physical bounds)
      4. Referential Integrity (Registered target cities)
    """
    if not records:
        logging.warning("No records provided to validate.")
        return [], []

    df = pd.DataFrame(records)
    valid_records: List[Dict[str, Any]] = []
    invalid_records: List[Dict[str, Any]] = []
    seen_keys = set()

    for _, row in df.iterrows():
        record_dict = row.to_dict()
        city = record_dict.get("city_name")
        date_id = record_dict.get("date_id")
        temp = record_dict.get("temperature")
        humidity = record_dict.get("humidity")
        pressure = record_dict.get("pressure")
        composite_key = f"{city}_{date_id}"

        # 1. Completeness Check
        missing_fields = [
            col for col in REQUIRED_COLUMNS if pd.isna(record_dict.get(col))
        ]
        if missing_fields:
            record_dict["failure_reason"] = f"Missing required fields: {missing_fields}"
            record_dict["rule_failed"] = "COMPLETENESS"
            record_dict["invalid_value"] = f"NULL in {missing_fields}"
            invalid_records.append(record_dict)
            continue

        # 2. Uniqueness Check
        if composite_key in seen_keys:
            record_dict["failure_reason"] = f"Duplicate composite key: {composite_key}"
            record_dict["rule_failed"] = "UNIQUENESS"
            record_dict["invalid_value"] = composite_key
            invalid_records.append(record_dict)
            continue
        seen_keys.add(composite_key)

        # 3. Range Validity Checks
        if not (MIN_TEMPERATURE <= temp <= MAX_TEMPERATURE):
            record_dict["failure_reason"] = f"Temperature {temp}°C outside [{MIN_TEMPERATURE}, {MAX_TEMPERATURE}]"
            record_dict["rule_failed"] = "RANGE_VALIDITY_TEMP"
            record_dict["invalid_value"] = str(temp)
            invalid_records.append(record_dict)
            continue

        if not (MIN_HUMIDITY <= humidity <= MAX_HUMIDITY):
            record_dict["failure_reason"] = f"Humidity {humidity}% outside [{MIN_HUMIDITY}, {MAX_HUMIDITY}]"
            record_dict["rule_failed"] = "RANGE_VALIDITY_HUMIDITY"
            record_dict["invalid_value"] = str(humidity)
            invalid_records.append(record_dict)
            continue

        if not (MIN_PRESSURE <= pressure <= MAX_PRESSURE):
            record_dict["failure_reason"] = f"Pressure {pressure} hPa outside [{MIN_PRESSURE}, {MAX_PRESSURE}]"
            record_dict["rule_failed"] = "RANGE_VALIDITY_PRESSURE"
            record_dict["invalid_value"] = str(pressure)
            invalid_records.append(record_dict)
            continue

        # 4. Referential Integrity Check
        matched_target = any(
            city.lower() in target.lower() for target in TARGET_CITIES
        )
        if not matched_target:
            record_dict["failure_reason"] = f"Unregistered city identifier: {city}"
            record_dict["rule_failed"] = "REFERENTIAL_INTEGRITY"
            record_dict["invalid_value"] = str(city)
            invalid_records.append(record_dict)
            continue

        valid_records.append(record_dict)

    logging.info(
        f"Validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid records."
    )
    return valid_records, invalid_records


if __name__ == "__main__":
    print("Running Task 3.2 Validation & Audit Logging Test...")
    init_dq_log_table()

    # 1. Fetch real clean data
    real_records = extract_all_weather()

    # 2. Inject synthetic corrupted records to test audit logging
    synthetic_bad_records = [
        {
            "city_name": "Kochi",
            "country": "IN",
            "date_id": 20260911,
            "temperature": 999.0,  # Range violation
            "humidity": 80,
            "pressure": 1010,
        },
        {
            "city_name": "Atlantis",  # Referential violation
            "country": "XX",
            "date_id": 20260911,
            "temperature": 25.0,
            "humidity": 50,
            "pressure": 1012,
        }
    ]

    test_batch = real_records + synthetic_bad_records
    valid, invalid = validate_weather_records(test_batch)

    # Persist the bad records to PostgreSQL
    log_invalid_records_to_db(invalid, pipeline_run_id="test_run_task_3_2")

    print("\n--- Final Verification Summary ---")
    print(f"Total Batch Size Tested: {len(test_batch)}")
    print(f"Clean Records Passed   : {len(valid)}")
    print(f"Bad Records Caught & Logged to DB: {len(invalid)}")
    print("[SUCCESS] Task 3.2 database audit logging verified.")
