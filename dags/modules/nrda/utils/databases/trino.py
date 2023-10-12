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


def trino_copy_table_to_iceberg(src_connector: str, src_schema: str, dst_connector: str,
                                dst_schema: str, table: str, bucket: str, engine: sqlalchemy.engine.Engine) -> None:
    """Creates a table in the iceberg connector from a table in the hive connector
    :param src_connector: String for the name of the hive connector
    :param src_schema: String for the name of the hive schema
    :param dst_connector: String for the name of the iceberg connector
    :param dst_schema: String for the name of the iceberg schema
    :param table: String for the name of the database table.
    :param bucket: String for the name of the s3 bucket used to store the trino data.
    :param engine: SqlAlchemy engine object for communicating to a database.
    """
    logger.info("Moving data from hive to iceberg")
    logger.debug(f"schema={dst_schema}")
    logger.debug(f"table={table}")

    query = f'''
    CREATE TABLE IF NOT EXISTS {dst_connector}.{dst_schema}.{table}
    WITH (
        location = 's3a://{bucket}/{table}/',
        format = 'PARQUET'
    )
    AS
        SELECT *
        FROM {src_connector}.{src_schema}.{table}
    '''

    trino_execute_query(engine, query)


def trino_create_schema(connector: str, schema: str, s3_bucket: str, engine: sqlalchemy.engine.Engine) -> None:
    """Create a schema in trino.
    :param connector: String for the name of the connector
    :param schema: String for the name of the database schema.
    :param s3_bucket: String for the path to a s3 bucket
    :param engine: SqlAlchemy engine object for communicating to a database
    """
    from modules.nrda.utils.s3 import create_bucket_if_not_exists, make_s3_client

    logger.info("Creating schema if does not exist")
    logger.debug(f"schema={schema}")
    logger.debug(f"s3_bucket={s3_bucket}")

    s3_client = make_s3_client()
    create_bucket_if_not_exists(s3_bucket, s3_client)

    query = f'''
    CREATE SCHEMA IF NOT EXISTS {connector}.{schema}
    WITH (location = 's3a://{s3_bucket}/')
    '''

    trino_execute_query(engine, query)


def trino_create_table_from_external_parquet_file(trino_engine: sqlalchemy.engine.Engine,
                                                  template_params: dict) -> None:
    """Templates a jinja2 file to create a table in trino based on the given params.
    :param trino_engine: SQLAlchemy engine to allow the communication of sql to trino.
    :param template_params: Dictionary of template parameters to be used in jinja2 templating.
    """
    import jinja2
    from airflow.models import Variable

    logger.info("Executing SQL to create table in trino")

    jinja_file_path = Variable.get("jinja_templates_file_path")
    template_file = f"{jinja_file_path}trino_create_table_from_external_parquet_file.jinja"
    with open(template_file) as fp:
        template = jinja2.Template(fp.read())
    formatted_sql = template.render(template_params)

    trino_execute_query(trino_engine, formatted_sql)


def trino_insert_values(trino_engine: sqlalchemy.engine.Engine, template_params: dict) -> None:
    """Templates a jinja2 file to insert data into a table in trino based on the given params.
    :param trino_engine: SQLAlchemy engine to allow the communication of sql to trino.
    :param template_params: Dictionary of template parameters to be used in jinja2 templating.
    """
    import jinja2

    logger.info("Executing SQL to add values to trino table")

    jinja_file_path = "/opt/airflow/dags/repo/dags/modules/nrda/jinja/"
    template_file = f"{jinja_file_path}trino_insert_data.jinja"
    with open(template_file) as fp:
        template = jinja2.Template(fp.read())
    formatted_sql = template.render(template_params)

    trino_execute_query(trino_engine, formatted_sql)