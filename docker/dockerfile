FROM apache/airflow:2.7.3-python3.11

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip

USER airflow

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r /requirements.txt