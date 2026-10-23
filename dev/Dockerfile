FROM apache/spark:4.0.2
ARG MVN=https://repo1.maven.org/maven2

USER root
RUN pip3 install --no-cache-dir \
    "pandas==2.3.3" \
    "pyarrow==25.0.1" \
    "grpcio==1.84.1" \
    "grpcio-status==1.84.1" \
    "elasticsearch>=9.5,<9.6"

USER spark

# --- Kafka (consummer / producer) ---
ADD --chown=spark:spark ${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.2/spark-sql-kafka-0-10_2.13-4.0.2.jar /opt/spark/jars/
ADD --chown=spark:spark ${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.2/spark-token-provider-kafka-0-10_2.13-4.0.2.jar /opt/spark/jars/
ADD --chown=spark:spark ${MVN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --chown=spark:spark ${MVN}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/spark/jars/

# --- S3A / MinIO ---
# ADD --chown=spark:spark ${MVN}/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar /opt/spark/jars/
# ADD --chown=spark:spark ${MVN}/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar /opt/spark/jars/

# --- Protobuf ---
ADD --chown=spark:spark ${MVN}/org/apache/spark/spark-protobuf_2.13/4.0.2/spark-protobuf_2.13-4.0.2.jar /opt/spark/jars/