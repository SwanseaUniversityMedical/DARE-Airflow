import logging
import sqlalchemy.engine

from ..utils.s3 import validate_s3_key
from ..utils.sql import validate_column, validate_identifier

logger = logging.getLogger(__name__)


def get_trino_conn_details(conn_name: str = 'trino_conn') -> dict:
    """Gets trino connection info from Airflow connection with connection id that is provided by the user.

    :param conn_name: A String for the name of the airflow connection for trino. Defaults to "trino_conn".
    :return: The credential information for the S3 connection that is needed to use Boto3 to connect.
    :rtype: dict
    """
    from airflow.hooks.base import BaseHook
    from airflow.exceptions import AirflowNotFoundException

    logger.info("Getting trino connection details")

    try:
        conn = BaseHook.get_connection(conn_name)
    except AirflowNotFoundException as e:
        logger.critical(f"There is no Airflow connection configured with the name '{conn_name}'.")
        raise e
    # Transform connection object into dictionary to make it easier to share between modules
    trino_conn = {"host": conn.host, "port": conn.port, "username": conn.login,
                  "password": conn.password, "database": conn.schema}

    return trino_conn


def get_trino_engine(trino_conn_details: dict) -> sqlalchemy.engine.Engine:
    """Creates a sqlalchemy engine for talking to trino.
    :param trino_conn_details: dict of information for connecting to trino database
    :return engine: sqlalchemy engine object

    """
    from sqlalchemy import create_engine
    import warnings

    logger.info("Creating engine to talk to trino")

    warnings.filterwarnings('ignore')

    username = trino_conn_details['username']
    host = trino_conn_details['host']
    port = trino_conn_details['port']
    database = trino_conn_details['database']

    logger.info(f"username={username}")
    logger.info(f"host={host}")
    logger.info(f"port={port}")
    logger.info(f"database={database}")

    engine = create_engine(
        f"trino://{username}@{host}:{port}/{database}",
        connect_args={
            "http_scheme": "http",
            # TODO This needs to be set to true when deploying to anything thats not dev
            "verify": False
        },
        echo=True
    )

    return engine


def trino_execute_query(engine: sqlalchemy.engine.Engine, query: str, **kwargs) -> None:
    """Executes SQL based on a provided query.
    :param query: String for the SQL query that will be executed
    :param engine: SqlAlchemy engine object for communicating to a database
    """
    try:
        logger.info("Trino query executing...")
        logger.info(f"query={query}")
        engine.execute(query, **kwargs)
        logger.info("Trino query success!")

    except Exception as ex:
        logger.exception("Trino query encountered an error!", exc_info=ex)
        raise ex


def create_schema(trino: sqlalchemy.engine.Engine, schema: str, location: str):
    query = f"CREATE SCHEMA IF NOT EXISTS " \
            f"{validate_identifier(schema)} " \
            f"WITH (" \
            f"location='s3a://{validate_s3_key(location)}/'" \
            f")"
    logger.info(f"trino={query}")
    trino.execute(query)


def drop_schema(trino: sqlalchemy.engine.Engine, schema: str):
    query = f"DROP SCHEMA " \
            f"{validate_identifier(schema)}"
    trino.execute(query)


def hive_create_table_from_csv(trino: sqlalchemy.engine.Engine, table: str, columns: list, location: str):
    schema = ", ".join(map(lambda col: f"{validate_column(col)} VARCHAR", columns))
    query = f"CREATE TABLE " \
            f"{validate_identifier(table)} ({schema}) " \
            f"WITH (" \
            f"external_location='s3a://{validate_s3_key(location)}', " \
            f"skip_header_line_count=1, " \
            f"format='CSV'" \
            f")"
    trino.execute(query)


def hive_create_table_from_parquet(trino: sqlalchemy.engine.Engine, table: str, location: str, schema: str):
    query = f"CREATE TABLE " \
            f"{validate_identifier(table)} " \
            f"({schema}) " \
            f"WITH (" \
            f"external_location='s3a://{validate_s3_key(location)}', " \
            f"format='PARQUET'" \
            f")"
    trino.execute(query)


def iceberg_create_table_from_hive(trino: sqlalchemy.engine.Engine, table: str, hive_table: str, location: str):

    query = f"CREATE TABLE " \
            f"{validate_identifier(table)} " \
            f"WITH (" \
            f"location='s3a://{validate_s3_key(location)}/', " \
            f"format='PARQUET'" \
            f") " \
            f"AS SELECT * FROM {validate_identifier(hive_table)}"
    trino.execute(query)


def drop_table(trino: sqlalchemy.engine.Engine, table: str):
    query = f"DROP TABLE " \
            f"{validate_identifier(table)}"
    trino.execute(query)
