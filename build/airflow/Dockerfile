FROM apache/airflow:3.3.1-python3.12

COPY build/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    && pip check

COPY --chown=airflow:root stages /opt/airflow/stages

ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/stages/airflow_dags
