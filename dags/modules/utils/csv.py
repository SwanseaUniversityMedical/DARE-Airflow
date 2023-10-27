import json
import logging
import pandas as pd
import pyarrow
import s3fs
import pyarrow.csv as pcsv
from airflow.hooks.base import BaseHook

from .s3 import s3_get_fs
from .sql import escape_column

logger = logging.getLogger(__name__)


def csv_get_columns_from_s3(conn_id: str, path: str, header=0, index_col=False) -> list:

    fs = s3_get_fs(conn_id)

    with fs.open(path, "rb") as fp:
        return pd.read_csv(fp, header=header, index_col=index_col, nrows=0).columns.tolist()


def csv_infer_schema_from_s3_pyarrow(conn_id: str, path: str):

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
