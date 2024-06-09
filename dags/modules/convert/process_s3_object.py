from dags.modules.convert.get_instructions import get_instructions
from dags.modules.convert.ingest_csv_to_iceberg import ingest_csv_to_iceberg
from dags.modules.utils.minioevent import decode_minio_event
from dags.modules.utils.version import compute_params
from random import randint
import logging
import json
import constants


def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)


def process_s3_object(bucket, key, etag, action):

    event = decode_minio_event(bucket, key, etag)
    logging.info(f"unpacked event={event}")

    # Based on the datasetname go and get an defined rules
    attribs, templates, duckdb_params, process, action, defaultUsed = get_instructions(event['dir_name'])
    # use bucket settings if no Dataset settings
    if defaultUsed:
        attribs, templates, duckdb_params, process, action, defaultUsed = get_instructions(f"BUCKET-{bucket}")

    logging.info(f'default uwsed = {defaultUsed}')
    logging.info(f'attributes = {attribs}')
    logging.info(f'templates = {templates}')
    logging.info(f'process = {process}')
    logging.info(f'action = {action}')
    logging.info(f'duckdb param = {duckdb_params}')

    # Compute paarmeters based on data and any templates defined
    params = compute_params(event,attribs,templates)
    logging.info(f"Computed Params = {params}")


    #tracking ={"process":{process}}
    tracking = {"process":{process},"attributes":{json.dumps(attribs)}, "templates":{json.dumps(templates)},"action":{action},"duckdb":{duckdb_params}, "params":{json.dumps(params)}}
    logging.info(f"tracking : {tracking}")
    
    # should we load data ?
    if action != constants.process_s3_option_whatif and process != constants.process_s3_formoption_no and ( action == constants.process_s3_option_manual or (process == constants.process_s3_formoption_yesauto and action == constants.process_s3_option_load )):

      
        tracking_str = "nothing"

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
                            debug=True,
                            tracking=tracking_str)
    else:
        logging.info("Instructions to NOT load")

        # update database - whatif

    # send rabbit MQ anyway

    