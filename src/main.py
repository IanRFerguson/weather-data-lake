from time import sleep

import duckdb

from config import WEATHER_MAP
from utilities.api import WeatherApiRunner
from utilities.general import logger
from utilities.iceberg import setup_iceberg, write_dataframe_to_iceberg

#####


def main():
    """
    Loops through a map of cities and their codes, fetches weather data via an API,
    standardizes it into a DataFrame, and writes it to an Iceberg table.
    """

    logger.info("Setting up local Iceberg catalog...")
    catalog = setup_iceberg()

    for city, code in WEATHER_MAP.items():
        logger.info(f"Running weather API for {city} (code: {code})...")

        # Create API runner instance
        runner = WeatherApiRunner(city_code=code)

        # Fetch and standardize weather data into DataFrame
        weather_df = runner.api_to_dataframe()

        # Write DataFrame to Iceberg table
        write_dataframe_to_iceberg(df=weather_df, city=city, catalog=catalog)

        # TODO - Move this to an exponential backoff strategy
        sleep(10)

    # Connect to DuckDB as the analytics engine and combine weather tables
    with duckdb.connect(database="/app/iceberg_catalog/pyiceberg_catalog.db") as conn:
        with open("/app/src/analytics/stack_tables.sql", "r") as f:
            query = f.read()
            logger.info("Running analytics query to stack tables...")
            conn.execute(query)
            result_df = conn.fetchdf()
            logger.debug(f"Analytics query result:\n{result_df}")


#####

if __name__ == "__main__":
    main()
