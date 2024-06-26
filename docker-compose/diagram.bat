
cd minio-trino
docker run --rm -it --name dcv -v .:/input pmsipilot/docker-compose-viz render -m image docker-compose.yml
cd ..\airflow
docker run --rm -it --name dcv -v .:/input pmsipilot/docker-compose-viz render -m image docker-compose.yml
cd ..
