import sys
import logging
from typing import Any, Dict, List, Tuple
import pandas as pd
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

# Critical fields required for completeness
REQUIRED_COLUMNS: List[str] = [
    "city_name",
    "date_id",
    "temperature",
    "humidity",
    "pressure"
]


def validate_weather_records(
    records: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Evaluates extracted records across 4 data quality pillars:
      1. Completeness (Null check)
      2. Uniqueness (Duplicate check)
      3. Range Validity (Physical meteorological boundaries)
      4. Referential Integrity (Known target cities)

    Returns:
      Tuple of (valid_records, invalid_records)
    """
    if not records:
        logging.warning("No records provided to validate.")
        return [], []

    df = pd.DataFrame(records)
    valid_records: List[Dict[str, Any]] = []
    invalid_records: List[Dict[str, Any]] = []
    seen_keys = set()

    for idx, row in df.iterrows():
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
            invalid_records.append(record_dict)
            continue

        # 2. Uniqueness Check
        if composite_key in seen_keys:
            record_dict["failure_reason"] = f"Duplicate composite key: {composite_key}"
            record_dict["rule_failed"] = "UNIQUENESS"
            invalid_records.append(record_dict)
            continue
        seen_keys.add(composite_key)

        # 3. Range Validity Checks
        if not (MIN_TEMPERATURE <= temp <= MAX_TEMPERATURE):
            record_dict["failure_reason"] = f"Temperature {temp}°C out of valid range [{MIN_TEMPERATURE}, {MAX_TEMPERATURE}]"
            record_dict["rule_failed"] = "RANGE_VALIDITY_TEMP"
            invalid_records.append(record_dict)
            continue

        if not (MIN_HUMIDITY <= humidity <= MAX_HUMIDITY):
            record_dict["failure_reason"] = f"Humidity {humidity}% out of valid range [{MIN_HUMIDITY}, {MAX_HUMIDITY}]"
            record_dict["rule_failed"] = "RANGE_VALIDITY_HUMIDITY"
            invalid_records.append(record_dict)
            continue

        if not (MIN_PRESSURE <= pressure <= MAX_PRESSURE):
            record_dict["failure_reason"] = f"Pressure {pressure} hPa out of valid range [{MIN_PRESSURE}, {MAX_PRESSURE}]"
            record_dict["rule_failed"] = "RANGE_VALIDITY_PRESSURE"
            invalid_records.append(record_dict)
            continue

        # 4. Referential Integrity Check
        # Clean city name check against target list
        matched_target = any(
            city.lower() in target.lower() for target in TARGET_CITIES
        )
        if not matched_target:
            record_dict["failure_reason"] = f"Unregistered city identifier: {city}"
            record_dict["rule_failed"] = "REFERENTIAL_INTEGRITY"
            invalid_records.append(record_dict)
            continue

        # If all checks pass
        valid_records.append(record_dict)

    logging.info(
        f"Validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid records."
    )
    return valid_records, invalid_records


if __name__ == "__main__":
    print("Testing data quality validation engine...")
    extracted_data = extract_all_weather()
    valid, invalid = validate_weather_records(extracted_data)

    print("\n--- Validation Summary ---")
    print(f"Total Records Tested : {len(extracted_data)}")
    print(f"Valid Records Passed : {len(valid)}")
    print(f"Invalid Records Caught: {len(invalid)}")

    if invalid:
        print("\n--- Caught Invalidation Details ---")
        for inv in invalid:
            print(f"[{inv['city_name']}] Failed: {inv['rule_failed']} -> {inv['failure_reason']}")

    print("\n[SUCCESS] Task 3.1 validation logic verified.")
