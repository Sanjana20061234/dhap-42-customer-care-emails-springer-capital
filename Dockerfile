FROM apache/airflow:2.10.5-python3.11

USER airflow

# Pinned dependencies matching Airflow 2.10.5 constraints
RUN pip install --no-cache-dir \
    "pandas==2.1.4" \
    "pyarrow==16.1.0" \
    "PyYAML==6.0.2"
