import pytest
from validate import validate_weather_records


@pytest.fixture
def clean_record_sample():
    """Provides a baseline valid weather record."""
    return {
        "city_name": "Kochi",
        "country": "IN",
        "latitude": 9.9399,
        "longitude": 76.2602,
        "date_id": 20260911,
        "observed_at": "2026-09-11T00:32:00+00:00",
        "temperature": 27.5,
        "feels_like": 29.0,
        "temp_min": 26.0,
        "temp_max": 28.5,
        "pressure": 1010,
        "humidity": 75,
        "wind_speed": 3.5,
        "wind_deg": 240,
    }


def test_validation_passes_clean_data(clean_record_sample):
    """Baseline Check: Clean record passes without rejection."""
    valid, invalid = validate_weather_records([clean_record_sample])
    assert len(valid) == 1
    assert len(invalid) == 0
    assert valid[0]["city_name"] == "Kochi"


def test_validation_catches_null_completeness(clean_record_sample):
    """Pillar 1 Check: Null/None values in critical columns are intercepted."""
    bad_record = clean_record_sample.copy()
    bad_record["temperature"] = None

    valid, invalid = validate_weather_records([bad_record])
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0]["rule_failed"] == "COMPLETENESS"


def test_validation_catches_duplicate_uniqueness(clean_record_sample):
    """Pillar 2 Check: Duplicate (city, date) observations are caught."""
    rec1 = clean_record_sample.copy()
    rec2 = clean_record_sample.copy()

    valid, invalid = validate_weather_records([rec1, rec2])
    assert len(valid) == 1
    assert len(invalid) == 1
    assert invalid[0]["rule_failed"] == "UNIQUENESS"


def test_validation_catches_temperature_out_of_range(clean_record_sample):
    """Pillar 3 Check: Temperatures exceeding boundaries (>60°C or <-50°C) are caught."""
    bad_record = clean_record_sample.copy()
    bad_record["temperature"] = 99.9  # Beyond meteorological range

    valid, invalid = validate_weather_records([bad_record])
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0]["rule_failed"] == "RANGE_VALIDITY_TEMP"


def test_validation_catches_humidity_out_of_range(clean_record_sample):
    """Pillar 3 Check: Relative humidity outside 0-100% is caught."""
    bad_record = clean_record_sample.copy()
    bad_record["humidity"] = 150  # Physically impossible relative humidity

    valid, invalid = validate_weather_records([bad_record])
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0]["rule_failed"] == "RANGE_VALIDITY_HUMIDITY"


def test_validation_catches_unregistered_city_referential(clean_record_sample):
    """Pillar 4 Check: Cities outside the registered target set fail referential integrity."""
    bad_record = clean_record_sample.copy()
    bad_record["city_name"] = "Atlantis"

    valid, invalid = validate_weather_records([bad_record])
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0]["rule_failed"] == "REFERENTIAL_INTEGRITY"
