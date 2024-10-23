def unpack_minio_event(message):
    import json
    message_json = json.loads(message)

    records = message_json["Records"][0]
    bucket = records["s3"]["bucket"]["name"]

    s3_object = records["s3"]["object"]
    etag = s3_object["eTag"]
    key = s3_object["key"]
    objsize = s3_object["size"]

    return bucket, key, etag, objsize


def decode_minio_event(bucket, key, etag):
  
    src_file_path: str = key.replace('%2F', '/')
    paths = src_file_path.split("/")
    extension = src_file_path.split("/")[-1].split(".")[1].lower()
    dir_name = paths[0]
    head_path = '/'.join(src_file_path.split('/')[:-1])
    filename = src_file_path.split("/")[-1].split(".")[0]

    version ='1'
    if len(paths) > 2:
        version = paths[-2]

    version = version.replace("-","_").replace("/","_")

    return dict(
        bucket=bucket,
        src_file_path=src_file_path,
        etag=etag,
        extension=extension,
        dir_name=dir_name,
        head_path=head_path,
        filename=filename,
        version=version,
        paths=paths
    )
