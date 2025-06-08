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


WORKDIR /opt/bitnami/spark