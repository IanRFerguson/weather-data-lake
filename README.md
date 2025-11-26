# Weather Data Lake

This is a simple implementation of a Data Lake, using Apache Iceberg for persisted storage and DuckDB for analytical querying. The `main.py` file hits the Weatherstack API for multiple cities, standardizes the output, and writes each rendered DataFrame to a corresponding Iceberg table in the catalog.

We then use DuckDB to run analytical SQL queries against the Iceberg tables, combining them into summary tables for analysis.