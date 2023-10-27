import os.path
import logging
import pendulum
from airflow import DAG
from airflow.operators.python import get_current_context, task

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


with DAG(
    dag_id="ingest_csv_to_iceberg",
    schedule=None,
    start_date=pendulum.datetime(1900, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    concurrency=1,
    tags=["ingest", "csv", "iceberg", "s3"],
) as dag:

    # Makes this logging namespace appear immediately in airflow
    logging.info("DAG parsing...")

    @task
    def ingest_csv_to_iceberg():

        ########################################################################
        logging.info("Starting task ingest_csv_to_iceberg...")

        # Extract the Airflow context object for this run of the DAG
        context = get_current_context()
        logging.debug(f"context={context}")

        # Extract the task instance object for handling XCOM variables
        ti = context['ti']
        logging.debug(f"ti={ti}")

        # Extract the JSON dict of params for this run of the DAG
        conf = context['dag_run'].conf
        logging.debug(f"conf={conf}")

        # Unique hashed name for this run of the DAG
        dag_hash = sha1(
            f"dag_id={ti.dag_id}/"
            f"run_id={ti.run_id}/"
            f"task_id={ti.task_id}"
        )
        logging.debug(f"dag_hash={dag_hash}")

        dag_id = f"{dag_hash}_{ti.try_number}"
        logging.debug(f"dag_id={dag_id}")

        ########################################################################
        logging.info("Validate inputs...")

        debug = conf.get("debug", False)
        logging.debug(f"debug={debug}")
        assert isinstance(debug, bool)

        # Path to the data file within the ingest bucket excluding the bucket name
        ingest_key = conf.get("ingest_key", None)
        logging.debug(f"ingest_key={ingest_key}")
        assert (ingest_key is not None) and \
               isinstance(ingest_key, str) and \
               ingest_key.endswith(".csv")

        ingest_path = os.path.dirname(ingest_key)
        ingest_file = os.path.basename(ingest_key)
        ingest_bucket = "ingest"

        ingest_delete = conf.get("ingest_delete", False)
        logging.debug(f"ingest_delete={ingest_delete}")
        assert isinstance(ingest_delete, bool)

        # Base name of the dataset to provision, defaults to an escaped version of the path
        dataset = escape_dataset(conf.get("dataset", ingest_path))

        ingest = {
            "bucket": ingest_bucket,
            "key": ingest_key,
            "path": ingest_path,
            "file": ingest_file,
            "dataset": dataset,
            "delete": ingest_delete,
        }
        logging.debug(f"ingest={ingest}")
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
        logging.debug(f"hive={hive}")
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
        logging.debug(f"iceberg={iceberg}")
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

    ingest_csv_to_iceberg()
