FROM python:3.10-slim

# Install JDK 21
RUN apt-get update && \
    apt-get install -y openjdk-21-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# -------------------------
# Create directory for preloaded jars
# -------------------------
# RUN mkdir -p /opt/spark-jars

# Copy all required jars into the image
# Place your downloaded jars in local folder `jars/` before building
# COPY spark-jars/ /opt/spark-jars/

# Add jars to Spark classpath (picked up by Spark Operator)
# ENV SPARK_CLASSPATH=/opt/spark-jars/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install h3

# Copy source code
COPY src/ /app/src/

# PYTHONPATH for Python modules
ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "src/hotel_data/pipeline/preprocessor/preprocessing_pipeline.py"]
