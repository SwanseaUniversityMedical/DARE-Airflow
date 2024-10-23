import dags.constants
from dags.modules.utils.version import attribute_search
import requests
import logging
import json
import redis
import constants
from airflow.hooks.base import BaseHook

def get_json_from_url(url):
    proxy = {
        'http': 'http://192.168.10.15:8080',
        'https': 'http://192.168.10.15:8080'
    }
    
    print(f"GETURL : {url}")
    
    #response = requests.get(url)
    response = requests.get(url,proxies=proxy)

    if response.status_code == 200:
        return response.json()
    else:
        return None

def get_json(url):
    redis_conn = BaseHook.get_connection('redis_conn')
    if redis_conn.password:
        redis_client = redis.Redis(host=redis_conn.host, port=redis_conn.port, db=0, username=redis_conn.login, password=redis_conn.password)
    else:
        redis_client = redis.Redis(host=redis_conn.host, port=redis_conn.port, db=0)

    # Check if JSON is in Redis
    json_data = redis_client.get(url)
    if json_data:
        # If JSON is in Redis, return it
        return json.loads(json_data)
    else:
        # If JSON is not in Redis, fetch from URL
        json_data = get_json_from_url(url)
        if json_data:
            # Store JSON in Redis with expiration time
            redis_client.setex(url, constants.redis_expiry, json.dumps(json_data))
        return json_data


def get_instructions(datasetname):

    defaultUsed = True

    # need to compute this
    url =  constants.assets3_url + datasetname
    logging.info(f'Getting loading instructions from {url}')

    # templates and default values if not changed
    templates = dict(
        version_template=r'''{% if (s3.version) and s3.version %}{{ s3.version}}{% else %}{{ attrib["att_version"][0] }}{% endif %}''',
        label_template = '{{ s3.filename }}',
        table_template = '{{ attrib["label"] }}_{{ attrib["version"] }}',
        dataset_template = '{{ s3.dir_name }}'
    )

    attribs = dict()
    duckdb_params = 'sample_size=-1,  ignore_errors=true'
    process = "yesAlways"
    action = "default"

    try:
        proxy = {
            'http': 'http://192.168.10.15:8080',
            'https': 'http://192.168.10.15:8080'
        }
        # Fetch JSON data from the URL and parse it into a Python variable
        response = requests.get(url, proxies=proxy)
        #response = requests.get(url)

        # Check if the response status code is OK (200)
        r_data = get_json(url)            
        logging.info(r_data)
        if r_data:
            
            hits = r_data.get("hits").get("total").get("value")
            logging.info(f"search hits = : {hits}")

            if hits == 1:  # single answer
                defaultUsed = False

                data = r_data.get("hits").get("hits")[0].get("_source")
                logging.info(data)

                process = data.get("process")

                if data.get("tableName"):
                    templates['table_template']=data.get("tableName")
                if data.get("version"):
                    templates['version_template']=data.get("version")
                if data.get("label"):
                    templates['label_template']=data.get("label")
                if data.get("dataset_template"):
                    templates['dataset_template']=data.get("dataset_template")

                ignore_errors = data.get("IgnoreErrors")
                if ignore_errors == 'default':
                    duckdb_params=""
                elif ignore_errors == 'False':
                    duckdb_params="ignore_errors=false,"
                else:
                    duckdb_params="ignore_errors=true,"

                header = data.get("header")
                if header == 'True':
                    duckdb_params = duckdb_params + ' header=true,'
                elif header == 'False':
                    duckdb_params = duckdb_params + ' header=false,'

                action = data.get("action")
                sampling = data.get("sampling")
                duckdb_params = duckdb_params + 'sample_size=' + str(sampling)

                logging.info(f"IgnoreErrors: {ignore_errors}")
                logging.info(f"header: {header}")
                logging.info(f"sampling: {sampling}")

                attributes = data.get("attributes")
                if attributes:
                    for attribute in attributes:
                        attribute_name = attribute.get("attributeName")
                        attribute_source = attribute.get("source")
                        attribute_regex = attribute.get("regex")
                        attribute_single = attribute.get("single")
                        attribs[attribute_name]= attribute_search(attribute_source,attribute_regex,attribute_single)
                else:
                    logging.info("No attributes found.")
            else:
                logging.error(f"Error should only have a single value back from URL, returned = {hits}")
        else:
            logging.error(f"Failed to fetch JSON data. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching JSON data: {e}")

    return attribs, templates, duckdb_params, process, action, defaultUsed