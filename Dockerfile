FROM apache/airflow:2.8.0-python3.11

USER root

# Install system dependencies including Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    build-essential \
    libsasl2-dev \
    libsasl2-modules \
    wget \
    procps \
    openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

# Download and install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN echo "Downloading Spark ${SPARK_VERSION}..." && \
    wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN echo "Extracting Spark archive..." && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN echo "Moving Spark to ${SPARK_HOME}..." && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

RUN echo "Cleaning up archive..." && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN echo "Setting permissions..." && \
    chown -R airflow:root ${SPARK_HOME}

# Download required JARs for S3A and Delta Lake
RUN echo "Downloading Hadoop AWS and dependencies..." && \
    cd ${SPARK_HOME}/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    chown airflow:root *.jar

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.6.0 \
    pyspark==3.5.0 \
    delta-spark==3.0.0 \
    boto3==1.34.32 \
    botocore==1.34.32 \
    minio==7.2.3 \
    pandas==2.1.4 \
    pyarrow==14.0.2

# Set environment variables
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

USER airflow
