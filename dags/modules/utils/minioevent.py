def unpack_minio_event(message):
    import json

    message_json = json.loads(message)

    records = message_json["Records"][0]

    s3_object = records["s3"]["object"]

    user = records["userIdentity"]["principalId"]
    bucket = records["s3"]["bucket"]["name"]
    etag = s3_object["eTag"]

    src_file_path: str = s3_object["key"].replace('%2F', '/')
    assert src_file_path.endswith(".csv")

    paths = src_file_path.split("/")
    file_name = src_file_path.replace(".csv", "")
    dir_name = paths[0]

    full_file_path = message_json['Key']
    head_path = '/'.join(full_file_path.split('/')[:-1])
    filename = full_file_path.split("/")[-1].split(".")[0]

    version ='1'
    if len(paths) > 2:
        version = paths[-2]

    return dict(
        user=user,
        bucket=bucket,
        src_file_path=src_file_path,
        etag=etag,
        file_name=file_name,
        dir_name=dir_name,
        full_file_path=full_file_path,
        head_path=head_path,
        filename=filename,
        version=version,
        paths=paths
    )
