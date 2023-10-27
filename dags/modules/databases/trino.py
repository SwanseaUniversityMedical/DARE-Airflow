
import re
import json
import logging

import boto3
import pandas as pd
import pyarrow
import s3fs
import pyarrow.csv as pcsv
import sqlalchemy.engine
from airflow.hooks.base import BaseHook
from botocore.config import Config

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
        logger.debug(f"query={query}")
        engine.execute(query, **kwargs)
        logger.info("Trino query success!")

    except Exception as ex:
        logger.exception("Trino query encountered an error!", exc_info=ex)
        raise ex


def escape_column(column):
    return re.sub(r'[^a-zA-Z0-9]+', '_', column).strip("_")


def validate_column(column):
    assert column == escape_column(column)
    return column


def escape_dataset(dataset):
    # TODO make this more sensible
    return escape_column(dataset)


def validate_identifier(identifier):
    # Validate the identifier is strictly one or more dot separated identifiers
    assert re.match(
        r'^(?:[a-z](?:[_a-z0-9]*[a-z0-9]|[a-z0-9]*)'
        r'(?:\.[a-z](?:[_a-z0-9]*[a-z0-9]|[a-z0-9]*)))*$',
        identifier,
        flags=re.IGNORECASE
    )

    return identifier


def validate_s3_key(key):
    # Validate the s3 key is strictly one or more slash separated keys
    assert re.match(
        r'^(?:[a-z0-9\-_]+)(?:/(?:[a-z0-9\-_]+))*$',
        key,
        flags=re.IGNORECASE
    )

    return key


def create_schema(trino: sqlalchemy.engine.Engine, schema: str, location: str):
    query = f"CREATE SCHEMA IF NOT EXISTS " \
            f"{validate_identifier(schema)} " \
            f"WITH (" \
            f"location='s3a://{validate_s3_key(location)}/'" \
            f")"
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


def iceberg_create_table_from_hive(trino: sqlalchemy.engine.Engine, table: str, hive_table: str, columns: list, dtypes: dict, location: str):
    assert all(map(validate_column, columns))

    def cast_if_needed(col):
        if col in dtypes:
            return f"CAST({col} AS {dtypes[col]}) AS {col}"
        return col

    schema = ", ".join(map(cast_if_needed, columns))

    query = f"CREATE TABLE " \
            f"{validate_identifier(table)} " \
            f"WITH (" \
            f"location='s3a://{validate_s3_key(location)}/', " \
            f"format='PARQUET'" \
            f") " \
            f"AS SELECT {schema} FROM {validate_identifier(hive_table)}"
    trino.execute(query)


def drop_table(trino: sqlalchemy.engine.Engine, table: str):
    query = f"DROP TABLE " \
            f"{validate_identifier(table)}"
    trino.execute(query)


def s3_get_csv_columns(conn_id: str, path: str, header=0, index_col=False) -> list:

    s3_conn = json.loads(BaseHook.get_connection(conn_id).get_extra())

    fs = s3fs.S3FileSystem(
        endpoint_url=s3_conn["endpoint_url"],
        key=s3_conn["aws_access_key_id"],
        secret=s3_conn["aws_secret_access_key"],
        use_ssl=False
    )

    with fs.open(path, "rb") as fp:
        return pd.read_csv(fp, header=header, index_col=index_col, nrows=0).columns.tolist()


def s3_copy(conn_id: str, src_bucket, dst_bucket, src_key, dst_key, move=False):

    s3_conn = json.loads(BaseHook.get_connection(conn_id).get_extra())

    s3 = boto3.resource(
        's3',
        aws_access_key_id=s3_conn["aws_access_key_id"],
        aws_secret_access_key=s3_conn["aws_secret_access_key"],
        endpoint_url=s3_conn["endpoint_url"],
        config=Config(signature_version='s3v4')
    )

    s3.Bucket(dst_bucket).copy({'Bucket': src_bucket, 'Key': src_key}, dst_key)

    if move:
        s3.Object(src_bucket, src_key).delete()


def s3_delete(conn_id: str, bucket, key):

    s3_conn = json.loads(BaseHook.get_connection(conn_id).get_extra())

    s3 = boto3.resource(
        's3',
        aws_access_key_id=s3_conn["aws_access_key_id"],
        aws_secret_access_key=s3_conn["aws_secret_access_key"],
        endpoint_url=s3_conn["endpoint_url"],
        config=Config(signature_version='s3v4')
    )

    s3.Object(bucket, key).delete()


def s3_infer_csv_schema_pyarrow(conn_id: str, path: str):

    s3_conn = json.loads(BaseHook.get_connection(conn_id).get_extra())

    fs = s3fs.S3FileSystem(
        endpoint_url=s3_conn["endpoint_url"],
        key=s3_conn["aws_access_key_id"],
        secret=s3_conn["aws_secret_access_key"],
        use_ssl=False
    )

    # "lazily" compute csv schema directly from s3
    # TODO it actually seems to be loading the whole file into ram... womp womp
    with fs.open(path, "rb") as fp:
        schema: pyarrow.lib.Schema = pcsv.open_csv(fp).schema

    logger.info(f"schema={schema}")

    # convert the pyarrow schema to a iceberg sql schema
    # TODO add other iceberg datatypes
    dtype_map = {
        "STRING": "VARCHAR"
    }

    # create a list of tuples of (name, dtype)
    # dtype is remapped if it is in the dtype_map
    columns = dict()
    for field in schema:
        column = escape_column(field.name)
        dtype = str(field.type).upper()
        dtype = dtype_map.get(dtype, dtype)
        columns[column] = dtype

    logger.info(f"columns={columns}")

    return columns
