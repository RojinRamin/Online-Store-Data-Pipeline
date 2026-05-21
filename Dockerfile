FROM docker.arvancloud.ir/apache/airflow:3.2.1

USER airflow

COPY wheels /wheels

RUN pip install --no-index --find-links=/wheels \
    apache-airflow-providers-mongo
