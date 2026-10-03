FROM public.ecr.aws/emr-serverless/spark/emr-7.1.0:latest

USER root
COPY build/spark/requirements.txt /tmp/requirements.txt
RUN python3.11 -m venv /opt/silver-venv \
    && /opt/silver-venv/bin/python -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && /opt/silver-venv/bin/python -m pip check
RUN curl -fsSL https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/3.2.2-spark_3.5/spark-snowflake_2.12-3.2.2-spark_3.5.jar -o /usr/lib/spark/jars/spark-snowflake_2.12-3.2.2-spark_3.5.jar \
    && echo "53e2fb0e76d7097378413a3d0ae8d3f7b0588f3d  /usr/lib/spark/jars/spark-snowflake_2.12-3.2.2-spark_3.5.jar" | sha1sum -c - \
    && curl -fsSL https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/4.1.0/snowflake-jdbc-4.1.0.jar -o /usr/lib/spark/jars/snowflake-jdbc-4.1.0.jar \
    && echo "4a57dabcb45bd3ad3ad6a4cf17a08196e63cfeb0  /usr/lib/spark/jars/snowflake-jdbc-4.1.0.jar" | sha1sum -c -
USER hadoop:hadoop
