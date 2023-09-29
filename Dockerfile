FROM apache/airflow:2.7.1
COPY requirements.txt /
USER airflow
RUN pip install -r /requirements.txt
USER root