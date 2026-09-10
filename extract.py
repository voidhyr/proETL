import sys
import logging
from typing import Dict, List, Optional
import requests
from config import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL

# Set up logging for ingestion tracking
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


def fetch_city_weather(city_query: str, timeout_seconds: int = 10) -> Optional[Dict]:
    """
    Fetches raw weather observation data for a single city from OpenWeatherMap API.
    Handles HTTP response statuses and connection timeouts gracefully.
    """
    url = build_weather_url(city_query)
    try:
        response = requests.get(url, timeout=timeout_seconds)
        status_code = response.status_code

        if status_code == 200:
            logging.info(f"Successfully fetched data for: {city_query}")
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


def extract_all_weather(cities: List[str] = TARGET_CITIES) -> List[Dict]:
    """
    Iterates over target cities and extracts raw weather payloads into an in-memory list.
    """
    extracted_records: List[Dict] = []
    logging.info(f"Starting weather extraction for {len(cities)} cities...")

    for city in cities:
        payload = fetch_city_weather(city)
        if payload:
            extracted_records.append(payload)

    logging.info(f"Extraction finished. Retrieved {len(extracted_records)}/{len(cities)} records successfully.")
    return extracted_records


if __name__ == "__main__":
    records = extract_all_weather()
    if not records:
        logging.error("Extraction returned 0 records. Check network connection and API key.")
        sys.exit(1)

    print("\n--- Extraction Sample Output ---")
    first_record = records[0]
    print(f"City: {first_record.get('name')}")
    print(f"Coordinates: {first_record.get('coord')}")
    print(f"Main Weather: {first_record.get('main')}")
    print(f"Wind: {first_record.get('wind')}")
    print(f"Timestamp (epoch): {first_record.get('dt')}")
    print("[SUCCESS] Task 2.2 extraction test completed.")
