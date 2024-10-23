import os
import json
import logging
import duckdb
from airflow.hooks.base import BaseHook

from ..utils.s3 import validate_s3_key, get_conn_details, detect_if_secure_endpoint

logger = logging.getLogger(__name__)


def s3_csv_to_parquet(conn_id: str, src_bucket: str, dst_bucket: str, src_key: str, dst_key: str, memory: int = 40):

    assert src_key.lower().endswith(".csv")
    assert dst_key.lower().endswith(".parquet")
    assert validate_s3_key(os.path.dirname(src_key))
    assert validate_s3_key(os.path.dirname(dst_key))

    s3_conn = get_conn_details(conn_id)
    access_key_id = s3_conn['aws_access_key_id']
    secret_access_key = s3_conn['aws_secret_access_key']
    endpoint = s3_conn["endpoint_url"].replace("http://", "").replace("https://", "")
    is_secure = detect_if_secure_endpoint(s3_conn["endpoint_url"])

    # original duckdb
    # con = duckdb.connect(database=':memory:')

    # try giving it some disk space ?
    db_path = '/tmp/database.db'
    con = duckdb.connect(database=db_path)

    query = f"INSTALL '/opt/duckdb/httpfs.duckdb_extension';" \
            f"LOAD httpfs;" \
            f"SET s3_endpoint='{endpoint}';" \
            f"SET s3_access_key_id='{access_key_id}';" \
            f"SET s3_secret_access_key='{secret_access_key}';" \
            f"SET s3_use_ssl={is_secure};" \
            f"SET preserve_insertion_order=False;" \
            f"SET s3_url_style='path';" \
            f"SET memory_limit='{memory}GB'"
    logger.info(f"query={query}")
    con.execute(query)

    query = f"COPY (SELECT * FROM 's3://{src_bucket}/{src_key}') " \
            f"TO 's3://{dst_bucket}/{dst_key}'" \
            f"(FORMAT PARQUET, CODEC 'SNAPPY', ROW_GROUP_SIZE 100000);"
    logger.info(f"query={query}")
    con.execute(query)


def file_csv_to_parquet(src_file: str, dest_file: str, duckdb_params: str, memory: int = 40):
    
    #con = duckdb.connect(database=':memory:')

    # try giving it some local storage / disk space ?
    db_path = '/tmp/database.db'
    con = duckdb.connect(database=db_path)

    query = f"SET memory_limit='{memory}GB'"
    logger.info(f"query={query}")
    con.execute(query)

    query = f"COPY (SELECT * FROM read_csv_auto('{src_file}', {duckdb_params})) " \
            f"TO '{dest_file}' " \
            f"(FORMAT PARQUET, CODEC 'SNAPPY', ROW_GROUP_SIZE 100000);"
    logger.info(f"query={query}")
    con.execute(query)
