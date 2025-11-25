import logging

iceberg_logger = logging.getLogger("iceberg_logger")
iceberg_logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(levelname)s - %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

iceberg_logger.addHandler(console_handler)
