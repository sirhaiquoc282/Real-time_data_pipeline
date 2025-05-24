FROM bitnami/spark:3.5.0

USER root

RUN rm -rf /var/lib/apt/lists/* && apt-get update
RUN apt-get install -y --no-install-recommends \
    curl \
    libssl3 \
    openssl && \
    apt-get clean
RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# RUN curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
#     && curl -O https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.4.1/mongo-spark-connector_2.12-10.4.1.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
#     && curl -O https://repo1.maven.org/maven2/org/mongodb/bson/4.10.2/bson-4.10.2.jar \
#     && curl -O https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.10.2/mongodb-driver-core-4.10.2.jar \
#     && curl -O https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.10.2/mongodb-driver-sync-4.10.2.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
#     && mv *.jar /opt/bitnami/spark/jars/

WORKDIR /opt/bitnami/spark