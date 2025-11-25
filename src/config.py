import os

WEATHER_MAP = {
    "BROOKLYN": 11231,
    "RICHMOND": 23220,
    "SAN FRANCISCO": 94103,
    "CHARLOTTE": 28202,
    "PHILADELPHIA": 19103,
}

API_KEY = os.environ["WEATHER_API_KEY"]
BASE_URL = "https://api.weatherstack.com/current?access_key={API_KEY}&query={CITY_CODE}"
ICEBERG_CATALOG_PATH = "/app/iceberg_catalog"
