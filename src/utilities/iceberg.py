import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog

from config import ICEBERG_CATALOG_PATH
from utilities.general import logger

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

    logger.debug(catalog.list_namespaces())
    if ("main",) not in catalog.list_namespaces():
        logger.debug("Creating 'main' namespace in Iceberg catalog...")
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
        logger.info(f"Creating Iceberg table for {city}...")
        table = catalog.create_table(
            identifier=table_identifier,
            schema=arrow_df.schema,
        )
    else:
        table = catalog.load_table(table_identifier)
        iceberg_schema = table.schema().as_arrow()

        # Check if schemas match
        if arrow_df.schema != iceberg_schema:
            logger.debug(f"Schema mismatch detected for {city}. Aligning schemas...")

            # Add missing columns with null values
            missing_cols = set(iceberg_schema.names) - set(arrow_df.schema.names)
            for col in missing_cols:
                field = iceberg_schema.field(col)
                null_array = pa.nulls(len(arrow_df), type=field.type)
                arrow_df = arrow_df.append_column(field, null_array)

            # Reorder columns to match Iceberg schema
            arrow_df = arrow_df.select(iceberg_schema.names)

            # Cast types column by column with safe casting
            columns = []
            for i, field in enumerate(iceberg_schema):
                col = arrow_df.column(i)
                if col.type != field.type:
                    # Use safe=False to allow lossy conversions or handle manually
                    try:
                        col = col.cast(field.type, safe=False)
                    except pa.ArrowInvalid as e:
                        logger.warning(
                            f"Cannot cast {field.name} from {col.type} to {field.type}: {e}"
                        )
                        # Keep original type or handle as needed
                columns.append(col)

            arrow_df = pa.Table.from_arrays(columns, schema=iceberg_schema)

            logger.debug(f"Schema aligned for {city}.")

    table.append(arrow_df)
    logger.info(f"Appended data to Iceberg table for {city}.")
