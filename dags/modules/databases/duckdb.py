import os
import json
import logging
import duckdb
from airflow.hooks.base import BaseHook

from ..utils.s3 import validate_s3_key

logger = logging.getLogger(__name__)


def s3_csv_to_parquet(conn_id: str, src_bucket: str, dst_bucket: str, src_key: str, dst_key: str, memory: int = 500):

    assert src_key.lower().endswith(".csv")
    assert dst_key.lower().endswith(".parquet")
    assert validate_s3_key(os.path.dirname(src_key))
    assert validate_s3_key(os.path.dirname(dst_key))

    con = duckdb.connect(database=':memory:')

    logger.debug(f"memory={memory}")
    con.execute(f"SET memory_limit='{memory}MB'")

    s3_conn = json.loads(BaseHook.get_connection(conn_id).get_extra())

    endpoint = s3_conn["endpoint_url"]\
        .replace("http://", "").replace("https://", "")

    con.execute(f"""
        LOAD httpfs;
        SET s3_endpoint='{endpoint}';
        SET s3_access_key_id='{s3_conn["aws_access_key_id"]}';
        SET s3_secret_access_key='{s3_conn["aws_secret_access_key"]}';
        SET s3_use_ssl=False;
        SET s3_url_style='path';
        """)

    con.execute(f"""
        COPY (SELECT * FROM 's3://{src_bucket}/{src_key}')
        TO 's3://{dst_bucket}/{dst_key}'
        (FORMAT PARQUET, CODEC 'SNAPPY', ROW_GROUP_SIZE 100000);
        """)
