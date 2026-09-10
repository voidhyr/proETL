import sys
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests
from config import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL

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
    Returns a clean list of standardized dictionaries.
    """
    flattened_records: List[Dict[str, Any]] = []
    logging.info(f"Starting weather extraction pipeline for {len(cities)} cities...")

    for city in cities:
        raw_payload = fetch_city_weather(city)
        if raw_payload:
            flattened = flatten_weather_record(raw_payload)
            flattened_records.append(flattened)

    logging.info(
        f"Extraction pipeline completed. Processed {len(flattened_records)}/{len(cities)} cities."
    )
    return flattened_records


if __name__ == "__main__":
    records = extract_all_weather()
    if not records:
        logging.error("Extraction failed: 0 records extracted.")
        sys.exit(1)

    print("\n--- Standardized Records Output ---")
    for rec in records:
        print(
            f"[{rec['city_name']}, {rec['country']}] "
            f"DateID: {rec['date_id']} | "
            f"Temp: {rec['temperature']}°C | "
            f"Humidity: {rec['humidity']}% | "
            f"Pressure: {rec['pressure']} hPa | "
            f"Wind: {rec['wind_speed']} m/s"
        )
    print("\n[SUCCESS] Milestone 2 (API Ingestion) successfully verified.")
