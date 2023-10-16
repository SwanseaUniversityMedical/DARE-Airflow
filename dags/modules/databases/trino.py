"""Module controlling different methods to access and use trino.

This module contains the methods to that will allow us to preform different actions on different databases that
are being used in production.

This file can be imported as a module and contains the following functions:

    * get_trino_conn_details
    * get_trino_engine
    * trino_execute_query
    * trino_copy_table_to_iceberg
    * trino_create_schema
    * trino_create_table_from_external_parquet_file
    * trino_insert_values
"""
import logging
import sqlalchemy.engine

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

    logger.debug(f"username={username}")
    logger.debug(f"host={host}")
    logger.debug(f"port={port}")
    logger.debug(f"database={database}")

    engine = create_engine(
        f"trino://{username}@{host}:{port}/{database}",
        connect_args={
            "http_scheme": "https",
            # TODO This needs to be set to true when deploying to anything thats not dev
            "verify": False
        }
    )

    return engine


def trino_execute_query(engine: sqlalchemy.engine.Engine, query: str) -> None:
    """Executes SQL based on a provided query.
    :param query: String for the SQL query that will be executed
    :param engine: SqlAlchemy engine object for communicating to a database
    """
    engine.execute(query)
    logger.info("Trino query successfully executed")
