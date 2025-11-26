INSTALL iceberg;
LOAD iceberg;

SET unsafe_enable_version_guessing = true;

CREATE OR REPLACE TABLE weather_analytics AS (
    SELECT *, 'brooklyn' AS city FROM iceberg_scan('/app/iceberg_catalog/main/brooklyn_weather')
    UNION ALL
    SELECT *, 'richmond' AS city FROM iceberg_scan('/app/iceberg_catalog/main/richmond_weather')
    UNION ALL
    SELECT *, 'san francisco' AS city FROM iceberg_scan('/app/iceberg_catalog/main/san francisco_weather')
    UNION ALL
    SELECT *, 'charlotte' AS city FROM iceberg_scan('/app/iceberg_catalog/main/charlotte_weather')
    UNION ALL
    SELECT *, 'philadelphia' AS city FROM iceberg_scan('/app/iceberg_catalog/main/philadelphia_weather')
);