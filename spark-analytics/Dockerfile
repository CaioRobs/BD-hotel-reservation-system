FROM python:3.9-slim

ENV SPARK_VERSION=3.3.0 \
    HADOOP_VERSION=3 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get update && \
    apt-get install -y curl openjdk-17-jre-headless scala && \
    apt-get clean

RUN curl -L -o spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar xvf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

RUN pip install pyspark kafka-python

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

COPY ./reservation_analytics.py /app/reservation_analytics.py

WORKDIR /app

# ENTRYPOINT ["spark-submit", "/app/reservation_analytics.py"]
