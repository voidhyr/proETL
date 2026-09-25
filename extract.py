import sys
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests
from config import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL
from config import OPENWEATHER_AIR_POLLUTION_URL

# Configure standardized logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Target Indian cities monitored by the daily pipeline
TARGET_CITIES: List[str] = [
    "Kochi,IN",
    "Kozhikode,IN",
    "Bengaluru,IN",
    "Mumbai,IN",
    "Delhi,IN"
]


def build_weather_url(city_query: str, api_key: str = OPENWEATHER_API_KEY) -> str:
    """
    Constructs the OpenWeatherMap REST API request URL for a given city.
    Uses metric units (Celsius, m/s).
    """
    if not api_key:
        raise ValueError(
            "OPENWEATHER_API_KEY is not set. Verify your local .env configuration."
        )
    return f"{OPENWEATHER_BASE_URL}?q={city_query}&appid={api_key}&units=metric"


def fetch_city_weather(city_query: str, timeout_seconds: int = 10) -> Optional[Dict[str, Any]]:
    """
    Fetches raw weather observation data for a single city from OpenWeatherMap API.
    Handles HTTP response statuses and connection timeouts gracefully.
    """
    url = build_weather_url(city_query)
    try:
        response = requests.get(url, timeout=timeout_seconds)
        status_code = response.status_code

        if status_code == 200:
            logging.info(f"Successfully fetched raw data for: {city_query}")
            return response.json()
        elif status_code == 401:
            logging.error(f"401 Unauthorized for {city_query}. Check your OPENWEATHER_API_KEY.")
        elif status_code == 404:
            logging.warning(f"404 Not Found: City '{city_query}' not recognized by OpenWeatherMap.")
        elif status_code == 429:
            logging.warning(f"429 Rate Limit Exceeded while querying {city_query}.")
        elif status_code >= 500:
            logging.error(f"Server error ({status_code}) from OpenWeatherMap for {city_query}.")
        else:
            logging.error(f"Unexpected status code {status_code} for {city_query}: {response.text}")

        return None

    except requests.exceptions.Timeout:
        logging.error(f"Network timeout ({timeout_seconds}s) reached while querying {city_query}.")
        return None
    except requests.exceptions.ConnectionError:
        logging.error(f"Connection failed while reaching OpenWeatherMap for {city_query}.")
        return None
    except requests.exceptions.RequestException as err:
        logging.error(f"Unexpected request error for {city_query}: {err}")
        return None


def flatten_weather_record(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flattens and extracts relevant fields from the raw nested OpenWeatherMap JSON payload.
    Produces a standardized dictionary structure for validation and transformation.
    """
    coord = raw_payload.get("coord", {})
    main = raw_payload.get("main", {})
    wind = raw_payload.get("wind", {})
    sys_data = raw_payload.get("sys", {})

    # Convert observation epoch timestamp to ISO UTC string and date integer
    epoch_ts = raw_payload.get("dt")
    if epoch_ts:
        dt_obj = datetime.fromtimestamp(epoch_ts, tz=timezone.utc)
        observed_at = dt_obj.isoformat()
        date_id = int(dt_obj.strftime("%Y%m%d"))
    else:
        observed_at = None
        date_id = None

    return {
        "city_name": raw_payload.get("name"),
        "country": sys_data.get("country"),
        "latitude": coord.get("lat"),
        "longitude": coord.get("lon"),
        "date_id": date_id,
        "observed_at": observed_at,
        "temperature": main.get("temp"),
        "feels_like": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "pressure": main.get("pressure"),
        "humidity": main.get("humidity"),
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg")
    }


def extract_all_weather(cities: List[str] = TARGET_CITIES) -> List[Dict[str, Any]]:
    """
    Orchestrates extraction and flattening across all target cities.
    Fetches weather data and uses its coordinates to fetch air pollution metrics.
    """
    flattened_records: List[Dict[str, Any]] = []
    logging.info(f"Starting multi-stream extraction (Weather + Pollution) for {len(cities)} cities...")

    for city in cities:
        raw_weather = fetch_city_weather(city)
        if not raw_weather:
            continue

        # Extract coordinates directly from weather response
        coord = raw_weather.get("coord", {})
        lat = coord.get("lat")
        lon = coord.get("lon")

        raw_pollution = None
        if lat is not None and lon is not None:
            raw_pollution = fetch_city_air_pollution(lat, lon)

        # Combine weather and pollution into a single standardized record
        flattened = flatten_weather_and_pollution(raw_weather, raw_pollution)
        flattened_records.append(flattened)

    logging.info(
        f"Extraction pipeline completed. Processed {len(flattened_records)}/{len(cities)} cities."
    )
    return flattened_records

def fetch_city_air_pollution(lat: float, lon: float, timeout_seconds: int = 10) -> Optional[Dict[str, Any]]:
    """Fetches real-time Air Pollution data for given coordinates."""
    url = f"{OPENWEATHER_AIR_POLLUTION_URL}?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
    try:
        response = requests.get(url, timeout=timeout_seconds)
        if response.status_code == 200:
            return response.json()
        logging.warning(f"Pollution API failed with status: {response.status_code}")
        return None
    except requests.exceptions.RequestException as err:
        logging.error(f"Air Pollution API error: {err}")
        return None


def flatten_weather_and_pollution(raw_weather: Dict[str, Any],
                                  raw_pollution: Optional[Dict[str, Any]]) -> Dict[
    str, Any]:
    # Retain your existing weather flattening logic...
    record = flatten_weather_record(raw_weather)

    # Extract pollutant concentrations and AQI
    if raw_pollution and "list" in raw_pollution and len(raw_pollution["list"]) > 0:
        pollution_item = raw_pollution["list"][0]
        record["aqi"] = pollution_item.get("main", {}).get(
            "aqi")  # 1 (Good) to 5 (Very Poor)
        components = pollution_item.get("components", {})
        record["pm2_5"] = components.get("pm2_5")
        record["pm10"] = components.get("pm10")
        record["co"] = components.get("co")
        record["no2"] = components.get("no2")
    else:
        record["aqi"] = None
        record["pm2_5"] = None
        record["pm10"] = None
        record["co"] = None
        record["no2"] = None

    return record


if __name__ == "__main__":
    records = extract_all_weather()
    if not records:
        logging.error("Extraction failed: 0 records extracted.")
        sys.exit(1)

    print("\n--- Standardized Records Output (Weather + Air Pollution) ---")
    for rec in records:
        print(
            f"[{rec['city_name']}, {rec['country']}] "
            f"DateID: {rec['date_id']} | "
            f"Temp: {rec['temperature']}°C | "
            f"AQI: {rec.get('aqi')} | "
            f"PM2.5: {rec.get('pm2_5')} µg/m³ | "
            f"PM10: {rec.get('pm10')} µg/m³"
        )
    print("\n[SUCCESS] Multi-stream ingestion (Weather + Pollution) successfully verified.")
