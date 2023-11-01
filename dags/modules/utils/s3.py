import re
import json
import logging
import boto3
import s3fs
from airflow.hooks.base import BaseHook
from botocore.config import Config

logger = logging.getLogger(__name__)


def validate_s3_key(key):
    # Validate the s3 key is strictly one or more slash separated keys
    assert re.match(
        r'^(?:[a-z0-9\-_]+)(?:/(?:[a-z0-9\-_]+))*$',
        key,
        flags=re.IGNORECASE
    )

    return key


def s3_get_resource(conn_id: str):
    s3_conn = json.loads(BaseHook.get_connection(conn_id).get_extra())

    return boto3.resource(
        's3',
        aws_access_key_id=s3_conn["aws_access_key_id"],
        aws_secret_access_key=s3_conn["aws_secret_access_key"],
        endpoint_url=s3_conn["endpoint_url"],
        config=Config(signature_version='s3v4')
    )


def s3_get_fs(conn_id):

    s3_conn = json.loads(BaseHook.get_connection(conn_id).get_extra())

    return s3fs.S3FileSystem(
        endpoint_url=s3_conn["endpoint_url"],
        key=s3_conn["aws_access_key_id"],
        secret=s3_conn["aws_secret_access_key"],
        use_ssl=False
    )


def s3_copy(conn_id: str, src_bucket, dst_bucket, src_key, dst_key, move=False):

    s3 = s3_get_resource(conn_id)

    s3.Bucket(dst_bucket).copy({'Bucket': src_bucket, 'Key': src_key}, dst_key)

    if move:
        s3.Object(src_bucket, src_key).delete()


def s3_delete(conn_id: str, bucket, key):

    s3_get_resource(conn_id).Object(bucket, key).delete()
