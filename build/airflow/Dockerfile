FROM apache/airflow:slim-3.3.1-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends graphviz \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY build/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    && pip check

COPY --chown=airflow:root stages /opt/airflow/stages

ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/stages/airflow_dags
ENV PYTHONPATH=/opt/airflow
