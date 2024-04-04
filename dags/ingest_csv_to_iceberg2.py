import datetime
import hashlib
import os.path
import logging
import pendulum
import json
from airflow import DAG
from airflow.operators.python import get_current_context, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook

from modules.databases.duckdb import s3_csv_to_parquet
from modules.utils.s3 import s3_delete
from modules.utils.sql import escape_dataset
from modules.utils.sha1 import sha1
from modules.databases.trino import (
    create_schema,
    drop_table,
    get_trino_conn_details,
    get_trino_engine,
    hive_create_table_from_parquet,
    iceberg_create_table_from_hive,
    validate_identifier,
    validate_s3_key
)

def unpack_minio_event(message):
    import json

    logging.info("Decoding minio event as JSON")
    message_json = json.loads(message)

    records = message_json["Records"][0]

    s3_object = records["s3"]["object"]

    user = records["userIdentity"]["principalId"]
    bucket = records["s3"]["bucket"]["name"]
    etag = s3_object["eTag"]

    src_file_path: str = s3_object["key"].replace('%2F', '/')
    assert src_file_path.endswith(".csv")

    file_name = src_file_path.replace(".csv", "")
    dir_name = src_file_path.split("/")[0]

    full_file_path = message_json['Key']
    head_path = '/'.join(full_file_path.split('/')[:-1])

    return dict(
        user=user,
        bucket=bucket,
        src_file_path=src_file_path,
        etag=etag,
        file_name=file_name,
        dir_name=dir_name,
        full_file_path=full_file_path,
        head_path=head_path
    )


def sha1(value):
    sha_1 = hashlib.sha1()
    sha_1.update(str(value).encode('utf-8'))
    return sha_1.hexdigest()

def ingest_csv_to_iceberg(dataset, ingest_key, ingest_delete, debug):

    ########################################################################
    logging.info("Starting function ingest_csv_to_iceberg...")

    # Extract the Airflow context object for this run of the DAG
    context = get_current_context()
    logging.info(f"context={context}")

    # Extract the task instance object for handling XCOM variables
    ti = context['ti']
    logging.info(f"ti={ti}")

    # Extract the JSON dict of params for this run of the DAG
    conf = context['dag_run'].conf
    logging.info(f"conf={conf}")

    # Unique hashed name for this run of the DAG
    dag_hash = sha1(
        f"dag_id={ti.dag_id}/"
        f"run_id={ti.run_id}/"
        f"task_id={ti.task_id}"
    )
    logging.info(f"dag_hash={dag_hash}")

    dag_id = f"{dag_hash}_{ti.try_number}"
    logging.info(f"dag_id={dag_id}")

    ########################################################################
    logging.info("Validate inputs...")

    #debug = conf.get("debug", False)
    logging.info(f"debug={debug}")
    assert isinstance(debug, bool)

    # Path to the data file within the ingest bucket excluding the bucket name
    #ingest_key = conf.get("ingest_key", None)
    logging.info(f"ingest_key={ingest_key}")
    assert (ingest_key is not None) and \
            isinstance(ingest_key, str) and \
            ingest_key.endswith(".csv")

    ingest_path = os.path.dirname(ingest_key)
    ingest_file = os.path.basename(ingest_key)
    ingest_bucket = "ingest"

    #ingest_delete = conf.get("ingest_delete", False)
    logging.info(f"ingest_delete={ingest_delete}")
    assert isinstance(ingest_delete, bool)

    # Base name of the dataset to provision, defaults to an escaped version of the path
    #dataset = escape_dataset(conf.get("dataset", ingest_path))

    ingest = {
        "bucket": ingest_bucket,
        "key": ingest_key,
        "path": ingest_path,
        "file": ingest_file,
        "dataset": dataset,
        "delete": ingest_delete,
    }
    logging.info(f"ingest={ingest}")
    ti.xcom_push("ingest", ingest)

    hive_schema = "minio.csv"
    hive_table = validate_identifier(f"{hive_schema}.{dataset}_{dag_id}")
    hive_bucket = "loading"
    hive_dir = validate_s3_key(f"ingest/{dataset}/{dag_id}")
    hive_file, _ = os.path.splitext(ingest_file)
    hive_key = f"{hive_dir}/{hive_file}.parquet"
    hive_path = validate_s3_key(f"{hive_bucket}/{hive_dir}")
    hive = {
        "schema": hive_schema,
        "table": hive_table,
        "bucket": hive_bucket,
        "dir": hive_dir,
        "file": hive_file,
        "key": hive_key,
        "path": hive_path,
    }
    logging.info(f"hive={hive}")
    ti.xcom_push("hive", hive)

    iceberg_schema = "iceberg.ingest"
    iceberg_table = validate_identifier(f"{iceberg_schema}.{dataset}_{dag_id}")
    iceberg_bucket = "working"
    iceberg_dir = validate_s3_key(f"ingest/{dataset}/{dag_id}")
    iceberg_path = validate_s3_key(f"{iceberg_bucket}/{iceberg_dir}")
    iceberg = {
        "schema": iceberg_schema,
        "table": iceberg_table,
        "bucket": iceberg_bucket,
        "dir": iceberg_dir,
        "path": iceberg_path,
    }
    logging.info(f"iceberg={iceberg}")
    ti.xcom_push("iceberg", iceberg)

    ########################################################################
    logging.info("Convert from ingest bucket CSV to loading bucket Parquet using DuckDB...")
    s3_csv_to_parquet(
        conn_id="s3_conn",
        src_bucket=ingest_bucket,
        src_key=ingest_key,
        dst_bucket=hive_bucket,
        dst_key=hive_key
    )

    if ingest_delete:
        s3_delete(conn_id="s3_conn", bucket=ingest_bucket, key=ingest_key)

    ########################################################################
    logging.info("Mounting CSV on s3 into Hive connector and copy to Iceberg...")

    # Create a connection to Trino
    trino_conn = get_trino_conn_details()
    trino = get_trino_engine(trino_conn)

    logging.info("Create schema in Hive connector...")
    create_schema(trino, schema=hive_schema, location=hive_bucket)

    logging.info("Create schema in Iceberg connector...")
    create_schema(trino, schema=iceberg_schema, location=iceberg_bucket)

    try:
        logging.info("Create table in Hive connector...")
        hive_create_table_from_parquet(
            trino,
            table=hive_table,
            location=hive_path
        )

        logging.info("Create table in Iceberg connector...")
        iceberg_create_table_from_hive(
            trino,
            table=iceberg_table,
            hive_table=hive_table,
            location=iceberg_path
        )

    finally:
        if debug:
            logging.info("Debug mode, not cleaning up table in Hive connector...")
        else:
            logging.info("Cleanup table in Hive connector...")
            drop_table(trino, table=hive_table)

            logging.info("Cleanup data from Hive connector in s3...")
            # External location data is not cascade deleted on drop table
            s3_delete(
                conn_id="s3_conn",
                bucket=hive_bucket,
                key=hive_key
            )

    ########################################################################


with DAG(
    dag_id="ingest_csv_to_iceberg2",
    schedule=None,
    start_date=pendulum.datetime(1900, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    concurrency=1,
    tags=["ingest", "csv", "iceberg", "s3"],
) as dag:

    # Makes this logging namespace appear immediately in airflow
    logging.info("DAG parsing...")

    def process_event(message):
        logging.info("Processing message!")
        logging.info(f"message={message}")

        context = get_current_context()
        ti = context['ti']
        dag_hash = sha1(
            f"dag_id={ti.dag_id}/"
            f"run_id={ti.run_id}/"
            f"task_id={ti.task_id}"
        )

        event = unpack_minio_event(message)
        logging.info(f"event={event}")

    consume_events = RabbitMQPythonOperator(
        func=process_event,
        task_id="consume_events",
        rabbitmq_conn_id="rabbitmq_conn",
        queue_name="airflow",
        deferrable=datetime.timedelta(seconds=120),
        poke_interval=datetime.timedelta(seconds=1),
        retry_delay=datetime.timedelta(seconds=10),
        retries=999999999,
    )

    # If the consumer task fails and isn't restarted, restart the whole DAG
    restart_dag = TriggerDagRunOperator(
        task_id="restart_dag",
        trigger_dag_id=dag.dag_id,
        trigger_rule=TriggerRule.ALL_DONE
    )

    consume_events >> restart_dag
