import pytest
from validate import validate_weather_records

def test_valid_data_passes():
    """Verify that a clean observation record passes validation completely."""
    clean_data = [{
        "city_name": "Kochi",
        "date_id": 20260911,
        "temperature": 28.5,
        "humidity": 80,
        "pressure": 1012,
        "feels_like": 31.0,
        "temp_min": 27.0,
        "temp_max": 29.5,
        "wind_speed": 3.2,
        "wind_deg": 240
    }]
    valid_df, invalid_records = validate_weather_records(clean_data)
    assert len(valid_df) == 1
    assert len(invalid_records) == 0

def test_range_rule_temperature_out_of_bounds():
    """Verify that impossible physical temperatures (> 60°C) are caught."""
    corrupted_data = [{
        "city_name": "Delhi",
        "date_id": 20260911,
        "temperature": 150.0,  # Impossible physical temperature (> 60°C)
        "humidity": 45,
        "pressure": 1005,
        "feels_like": 150.0,
        "temp_min": 140.0,
        "temp_max": 160.0,
        "wind_speed": 2.1,
        "wind_deg": 180
    }]
    valid_df, invalid_records = validate_weather_records(corrupted_data)
    assert len(valid_df) == 0
    assert len(invalid_records) > 0

def test_completeness_missing_city():
    """Verify that a missing or null city identifier is rejected."""
    corrupted_data = [{
        "city_name": None,
        "date_id": 20260911,
        "temperature": 25.0,
        "humidity": 65,
        "pressure": 1010,
        "feels_like": 25.0,
        "temp_min": 24.0,
        "temp_max": 26.0,
        "wind_speed": 1.5,
        "wind_deg": 90
    }]
    valid_df, invalid_records = validate_weather_records(corrupted_data)
    assert len(valid_df) == 0
    assert len(invalid_records) > 0
