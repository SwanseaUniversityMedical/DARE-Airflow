FROM apache/airflow:2.9.0-python3.12 as build

# Ensure that release notes get picked up from our repo and not from the apache airflow base repo
# If the apache airflow notes are used we will incorrectly pickup the release notes from our version
# string searched against their release history....
LABEL org.opencontainers.image.source=https://github.com/SwanseaUniversityMedical/DARE-Airflow

# Add trusted certificates
USER root
ADD containers/dags/tls-certs/chi-root-ca.pem /usr/local/share/ca-certificates/chi-root-ca.crt
ADD containers/dags/tls-certs/chi-domain-ca.pem /usr/local/share/ca-certificates/chi-domain-ca.crt
ADD containers/dags/tls-certs/bundle.pem /usr/local/share/ca-certificates/bundle.pem
ENV SSL_CERT_FILE="/usr/local/share/ca-certificates/bundle.pem"

RUN chmod 644 /usr/local/share/ca-certificates/bundle.pem && \
    chmod 644 /usr/local/share/ca-certificates/chi-root-ca.crt && \
    chmod 644 /usr/local/share/ca-certificates/chi-domain-ca.crt && \
    update-ca-certificates

USER airflow
ENV SSL_CERT_FILE="/usr/local/share/ca-certificates/bundle.pem"
ENV REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"
# Add directory to the python path so we can import modules nested within that directory
# DAGs injected via volume mount live in /opt/airflow/dags
# DAGs injected via git-sync live in /opt/airflow/dags/repo/dags
ENV PYTHONPATH="/opt/airflow/dags/modules:/opt/airflow/dags/repo/dags/modules:${PYTHONPATH}"

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
