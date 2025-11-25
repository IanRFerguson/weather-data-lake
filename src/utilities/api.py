from dataclasses import dataclass

import pandas as pd
import requests

from config import API_KEY, BASE_URL

#####


@dataclass
class WeatherApiRunner:
    city_code: int

    def fetch_weather(self) -> dict:
        """
        Hits the weather API and returns the JSON response.
        """

        url = BASE_URL.format(API_KEY=API_KEY, CITY_CODE=self.city_code)
        response = requests.get(url)
        response.raise_for_status()

        return response.json()

    def __standardize_weather_data(self, data: dict, parent_key: str = "") -> dict:
        """
        Recursively standardizes the raw weather data into a consistent format.

        Args:
            data (dict): Raw JSON data from the weather API.
            parent_key (str): The parent key for nested items.

        Returns:
            dict: Standardized, unnested weather data.
        """

        clean_data = {}

        for key, value in data.items():
            new_key = f"{parent_key}_{key}" if parent_key else key

            # NOTE - We'll call this function recursively to handle
            # nested dictionaries in the API response.
            if isinstance(value, dict):
                nested_data = self.__standardize_weather_data(
                    data=value, parent_key=new_key
                )
                clean_data.update(nested_data)

            else:
                clean_data[new_key] = value

        return clean_data

    def api_to_dataframe(self) -> pd.DataFrame:
        """
        Fetches weather data from the API and converts it into a standardized DataFrame.
        """

        # Get the raw data from the API
        weather_data = self.fetch_weather()

        # Tidy up the data into a standard format
        standardized_data = self.__standardize_weather_data(weather_data)

        return pd.DataFrame([standardized_data])
