import pandas as pd
import s3fs
import re

from psycopg2 import sql
import sqlalchemy
from sqlalchemy.engine import create_engine

fs = s3fs.S3FileSystem(
    endpoint_url="https://minio-api.epcc-teleport.dk.serp.ac.uk:80",
    key="zEg4gJG6tprK44O2m0XbJMmI",
    secret="UB0Ur5bIFGiNz8oRSav86SM6CFLCuJOPZgtZ",
    use_ssl=False
)

with fs.open("s3a://ingest/dilbert/iris.csv", "rb") as fp:
    columns = pd.read_csv(fp, header=0, index_col=False, nrows=0).columns.tolist()
    fp.close()


def escape_column(column):
    return re.sub(r'[^a-zA-Z0-9]', '_', column).strip(" \t\n_")

columns = list(map(escape_column, columns))
print(columns)

trino = create_engine(
    f"trino://admin@trino.epcc-teleport.dk.serp.ac.uk:443",
    connect_args={
        "http_scheme": "https",
        "verify": False
    },
    echo=True
)


def escape_chars(value):
    return re.sub(
        r'[^a-z0-9\.]+', '_',
        value,
        flags=re.IGNORECASE
    ).strip("_")
def validate_identifier(identifier):
    # Validate the identifier is strictly one or more dot separated identifiers
    assert re.match(
        r'^(?:[a-z](?:[_a-z0-9]*[a-z0-9]|[a-z0-9]*)'
        r'(?:\.[a-z](?:[_a-z0-9]*[a-z0-9]|[a-z0-9]*)))*$',
        identifier,
        flags=re.IGNORECASE
    )

    return identifier

def validate_s3_key(key):
    # Validate the s3 key is strictly one or more slash separated keys
    assert re.match(
        r'^(?:[a-z0-9\-_]+)(?:/(?:[a-z0-9\-_]+))*$',
        key,
        flags=re.IGNORECASE
    )

    return key

print(validate_s3_key("loading"))
print(validate_s3_key("loading/dilbert"))
print(validate_s3_key("loadin-g/-/dil!bert/hellow"))

# print(trino.execute('SELECT * FROM minio.load."{table}"'.format(table=sql.Identifier("dilbert\" LIMIT 2 --").)).fetchall())