import logging
import os

logger = logging.getLogger("iceberg_logger")
logger.setLevel(logging.INFO)

if os.environ.get("DEBUG", "false").lower() == "true":
    logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(levelname)s %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
