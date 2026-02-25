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
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Spark will be copied from spark-master at runtime
# Note: We use pip-installed PySpark (not /opt/spark/python) to ensure version consistency
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

USER airflow

# Install Python packages with matching versions
#RUN pip install --no-cache-dir \
#    apache-airflow-providers-apache-spark==4.6.0 \
#    pyspark==3.5.3 \
#    delta-spark==3.1.0 \
#    boto3==1.34.32 \
#    botocore==1.34.32 \
#    minio==7.2.3 \
#    pandas==2.1.4 \
#    pyarrow==14.0.2

# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install --force-reinstall "celery==5.3.6"

USER airflow
