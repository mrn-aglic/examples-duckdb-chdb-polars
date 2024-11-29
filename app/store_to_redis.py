import logging

from app import constants
from app.tools import chdb, duckdb, polars

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

pattern = constants.FILE_PATTERN

logger.info("Starting CSV to Redis for DuckDB")
duckdb.csv_to_redis(pattern=pattern)
logger.info("Finished CSV to Redis for DuckDB")


logger.info("Starting CSV to Redis for ChDB 1")
chdb.csv_to_redis(pattern=pattern)
logger.info("Finished CSV to Redis for ChDB 1")


logger.info("Starting CSV to Redis for ChDB 2")
chdb.csv_to_redis_table(pattern=pattern)
logger.info("Finished CSV to Redis for ChDB 2")


logger.info("Starting CSV to Redis for Polars")
polars.csv_to_redis(pattern=pattern)
logger.info("Finished CSV to Redis for Polars")

logger.info("Starting Polars SQL example")
polars.polars_sql_query(pattern)
logger.info("Finished Polars SQL example")
