from time import sleep

from config import WEATHER_MAP
from utilities.api import WeatherApiRunner
from utilities.general import iceberg_logger
from utilities.iceberg import setup_iceberg, write_dataframe_to_iceberg

#####


def main():
    """
    Loops through a map of cities and their codes, fetches weather data via an API,
    standardizes it into a DataFrame, and writes it to an Iceberg table.
    """

    iceberg_logger.info("Setting up local Iceberg catalog...")
    catalog = setup_iceberg()

    for city, code in WEATHER_MAP.items():
        iceberg_logger.info(f"Running weather API for {city} (code: {code})...")

        # Create API runner instance
        runner = WeatherApiRunner(city_code=code)

        # Fetch and standardize weather data into DataFrame
        weather_df = runner.api_to_dataframe()

        # Write DataFrame to Iceberg table
        write_dataframe_to_iceberg(df=weather_df, city=city, catalog=catalog)

        sleep(10)


#####

if __name__ == "__main__":
    main()
