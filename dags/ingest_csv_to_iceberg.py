import os.path
import logging
import pendulum
from airflow import DAG
from airflow.operators.python import get_current_context, task

from modules.utils.sha1 import sha1
from modules.databases.trino import (
    create_schema,
    drop_table,
    escape_column,
    escape_dataset,
    get_trino_conn_details,
    get_trino_engine,
    hive_create_table_from_csv,
    iceberg_create_table_from_hive,
    s3_copy, s3_delete, s3_get_csv_columns,
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

        # Determine if a schema was passed to the dag
        schema = conf.get("schema", None)
        logging.debug(f"schema={schema}")

        # Schema must be a dict or the word infer
        assert (schema is None) or \
               (isinstance(schema, dict)) or \
               (isinstance(schema, str) and schema.lower() in ["infer", "varchar"])

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
        hive_key = f"{hive_dir}/{ingest_file}"
        hive_path = validate_s3_key(f"{hive_bucket}/{hive_dir}")
        hive = {
            "schema": hive_schema,
            "table": hive_table,
            "bucket": hive_bucket,
            "dir": hive_dir,
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
        logging.info("Move the data from ingest to loading bucket...")
        s3_copy(
            conn_id="s3_conn",
            src_bucket=ingest_bucket,
            src_key=ingest_key,
            dst_bucket=hive_bucket,
            dst_key=hive_key,
            move=ingest_delete
        )

        ########################################################################
        logging.info("Peek at CSV on s3 to get column names...")
        columns = s3_get_csv_columns(
            conn_id="s3_conn",
            path=f"{hive_bucket}/{hive_key}"
        )
        logging.debug(f"columns={columns}")

        logging.info("Escape column names...")
        columns = list(map(escape_column, columns))
        logging.debug(f"columns={columns}")

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
            hive_create_table_from_csv(
                trino,
                table=hive_table,
                columns=columns,
                location=hive_path
            )

            logging.info("Create table in Iceberg connector...")
            iceberg_create_table_from_hive(
                trino,
                table=iceberg_table,
                hive_table=hive_table,
                columns=columns,
                location=iceberg_path
            )

        finally:
            if not debug:
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
