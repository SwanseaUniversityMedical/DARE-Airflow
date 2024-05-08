from dags.modules.convert.get_instructions import get_instructions
from dags.modules.convert.ingest_csv_to_iceberg import ingest_csv_to_iceberg
from dags.modules.utils.minioevent import decode_minio_event
from dags.modules.utils.version import compute_params
from random import randint
import logging

def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)


def process_s3_object(bucket, key, etag):

    event = decode_minio_event(bucket, key, etag)
    logging.info(f"unpacked event={event}")

    # Based on the datasetname go and get an defined rules
    attribs, templates, duckdb_params, process, action = get_instructions(event['dir_name'])
    logging.info(f'attributes = {attribs}')
    logging.info(f'templates = {templates}')
    logging.info(f'process = {process}')
    logging.info(f'action = {action}')
    logging.info(f'duckdb param = {duckdb_params}')

    if process == "yesAlways":

        # Compute paarmeters based on data and any templates defined
        params = compute_params(event,attribs,templates)
        logging.info(f"Computed Params = {params}")

        ingest_csv_to_iceberg(dataset=params['dataset'],
                            tablename=params["tablename"],
                            version=params["version"],
                            label=params["label"],
                            etag = event['etag'],
                            ingest_bucket=event['bucket'],
                            ingest_key=event['src_file_path'],
                            dag_id=event['etag']+str(random_with_N_digits(4)),
                            ingest_delete=False,
                            duckdb_params=duckdb_params,
                            action=action,
                            debug=True)
    else:
        logging.info("Instructions to abort processing")