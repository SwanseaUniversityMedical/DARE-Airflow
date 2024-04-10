FROM apache/airflow:2.9.0-python3.10 as build

# Ensure that release notes get picked up from our repo and not from the apache airflow base repo
# If the apache airflow notes are used we will incorrectly pickup the release notes from our version
# string searched against their release history....
LABEL org.opencontainers.image.source=https://github.com/SwanseaUniversityMedical/DARE-Airflow

# Add directory to the python path so we can import modules nested within that directory
# DAGs injected via volume mount live in /opt/airflow/dags
# DAGs injected via git-sync live in /opt/airflow/dags/repo/dags
ENV PYTHONPATH="/opt/airflow/dags/modules:/opt/airflow/dags/repo/dags/modules:${PYTHONPATH}"

USER airflow

# Install additional python dependencies
COPY containers/dags/requirements.txt .
RUN pip install --no-cache-dir pyclean && \
    pip install --no-cache-dir -r requirements.txt && \
    pyclean -v /home/airflow/.local && \
    pip uninstall -y pyclean

# Install duckdb extensions
RUN duckcli -D ":memory:" -e "INSTALL httpfs; LOAD httpfs" && \
    python3 -c "import duckdb; duckdb.connect(':memory:').sql('LOAD httpfs')"


# Copy dags code
COPY dags /opt/airflow/dags
