import re
import json
import logging
import boto3
from minio import Minio
#from minio.error import ResponseError
import s3fs
from airflow.hooks.base import BaseHook
from botocore.config import Config

logger = logging.getLogger(__name__)

def get_conn_details(conn_id) -> dict:
    """Gets S3 connection info from Airflow connection with given connection id

    :return: The credential information for the S3 connection that is needed to use Boto3 to connect.
    :rtype: dict
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        conn_extra = json.loads(BaseHook.get_connection(conn_id).get_extra())
    except AirflowNotFoundException as e:
        logger.critical("There is no Airflow connection configured with the name {0}.".format(conn_id))
        raise e
    # Transform connection object into dictionary to make it easier to share between modules
    s3_conn = {"endpoint_url": conn_extra["endpoint_url"], "aws_access_key_id": conn.login, "aws_secret_access_key": conn.password}
    return s3_conn

def validate_s3_key(key):
    # Validate the s3 key is strictly one or more slash separated keys
    logging.info(f"Validate key: {key}")
    assert re.match(
        r'^(?:[a-z0-9\-_]+)(?:/(?:[a-z0-9\-_]+))*$',
        key,
        flags=re.IGNORECASE
    )

    return key


def s3_get_resource(conn_id: str):
    s3_conn = get_conn_details(conn_id)

    return boto3.resource(
        's3',
        aws_access_key_id=s3_conn["aws_access_key_id"],
        aws_secret_access_key=s3_conn["aws_secret_access_key"],
        endpoint_url=s3_conn["endpoint_url"],
        config=Config(signature_version='s3v4')
    )


def s3_get_fs(conn_id):

    s3_conn = get_conn_details(conn_id)

    return s3fs.S3FileSystem(
        endpoint_url=s3_conn["endpoint_url"],
        key=s3_conn["aws_access_key_id"],
        secret=s3_conn["aws_secret_access_key"],
        use_ssl=True
    )


def s3_copy(conn_id: str, src_bucket, dst_bucket, src_key, dst_key, move=False):

    s3 = s3_get_resource(conn_id)

    s3.Bucket(dst_bucket).copy({'Bucket': src_bucket, 'Key': src_key}, dst_key)

    if move:
        s3.Object(src_bucket, src_key).delete()


def s3_delete(conn_id: str, bucket, key):

    s3_get_resource(conn_id).Object(bucket, key).delete()


def s3_create_bucket(conn_id: str, bucket):
    try:
        s3_get_resource(conn_id).create_bucket(Bucket=bucket.lower())
    except Exception as e:
        print(f"An error occurred while creating the S3 bucket: {e}")



def s3_download_minio(conn_id, bucket_name, object_name, local_file_path):

    s3_conn = get_conn_details(conn_id)

    url = str(s3_conn["endpoint_url"]).replace('http://','').replace('https://','')

    client = Minio(url,
               access_key=s3_conn["aws_access_key_id"],
               secret_key=s3_conn["aws_secret_access_key"],
               secure=True)
    
    
    #client.fget_object(bucket_name, object_name, local_file_path)
    
    #try:
    # Start a multipart download
    response = client.get_object(
        bucket_name,
        object_name,
        request_headers={"Range": "bytes=0-"}
    )

    # Open a file for writing
    with open(local_file_path, "wb") as file_data:
        # Iterate over the response data and write it to the file
        for data in response.stream(32 * 1024):
            file_data.write(data)

    print("Download successful!")

    #except ResponseError as err:
    #    print(err)


def s3_download(conn_id, bucket_name, object_name, local_file_path):

    s3_conn = get_conn_details(conn_id)

    client = boto3.client(
        's3',
        endpoint_url=s3_conn["endpoint_url"],
        aws_access_key_id=s3_conn["aws_access_key_id"],
        aws_secret_access_key=s3_conn["aws_secret_access_key"],
        use_ssl=True )

    with open(local_file_path, 'wb') as f:
        client.download_fileobj(bucket_name, object_name, f)


def s3_upload(conn_id, src_file, bucket, object_name):

    s3_conn = get_conn_details(conn_id)

    client = boto3.client(
        's3',
        endpoint_url=s3_conn["endpoint_url"],
        aws_access_key_id=s3_conn["aws_access_key_id"],
        aws_secret_access_key=s3_conn["aws_secret_access_key"],
        use_ssl=True )

    client.upload_file(src_file,bucket,object_name)
    