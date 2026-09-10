import sys
from typing import List
from config import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL

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


if __name__ == "__main__":
    print("Testing URL builder configuration for Indian cities...")
    for city in TARGET_CITIES:
        url = build_weather_url(city)
        # Mask the actual key for terminal safety
        masked_url = url.replace(OPENWEATHER_API_KEY, "HIDDEN_API_KEY")
        print(f"[{city}] -> {masked_url}")
    print("[SUCCESS] Target Indian cities and URL builder verified.")
