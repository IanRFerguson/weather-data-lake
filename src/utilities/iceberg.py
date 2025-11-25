import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog

from config import ICEBERG_CATALOG_PATH
from utilities.general import iceberg_logger

#####


def setup_iceberg() -> Catalog:
    """
    Docstring for setup_iceberg
    """

    catalog = load_catalog(
        namme="default",
        **{
            "type": "sql",
            "uri": f"sqlite:///{ICEBERG_CATALOG_PATH}/pyiceberg_catalog.db",
            "warehouse": f"file://{ICEBERG_CATALOG_PATH}",
        },
    )

    iceberg_logger.info(catalog.list_namespaces())
    if ("main",) not in catalog.list_namespaces():
        iceberg_logger.info("Creating 'main' namespace in Iceberg catalog...")
        catalog.create_namespace("main")

    return catalog


def write_dataframe_to_iceberg(df: pd.DataFrame, city: str, catalog: Catalog):
    """
    Writes the Pandas DataFrame to an Iceberg table named after the city.

    Args:
        df (pd.DataFrame): The DataFrame containing weather data.
        city (str): The name of the city, used as the table name.
    """

    table_identifier = f"main.{city.lower()}_weather"

    arrow_df = pa.Table.from_pandas(df)

    if not catalog.table_exists(table_identifier):
        iceberg_logger.info(f"Creating Iceberg table for {city}...")
        table = catalog.create_table(
            identifier=table_identifier,
            schema=arrow_df.schema,
        )
    else:
        table = catalog.load_table(table_identifier)

    table.append(arrow_df)
    iceberg_logger.info(f"Appended data to Iceberg table for {city}.")
