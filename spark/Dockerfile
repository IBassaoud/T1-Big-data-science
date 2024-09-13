FROM apache/spark-py:v3.2.4

USER root

RUN pip install pymongo matplotlib pandas

# Install netcat for the health check in the entrypoint script
RUN apt-get update && apt-get install -y netcat && rm -rf /var/lib/apt/lists/*

# Create necessary directories and set permissions
RUN mkdir -p /opt/spark/.ivy2 /tmp/spark-events /var/data && \
    chmod 777 /opt/spark/.ivy2 /tmp/spark-events /var/data

# Copy the custom entrypoint script
COPY entrypoint/init.sh /init_entrypoint.sh
RUN chmod +x /init_entrypoint.sh

# Copy the Python script
COPY datas/nutritional_index_classification.py /var/data/nutritional_index_classification.py

# Set the working directory
WORKDIR /opt/spark

# Use the custom entrypoint script
ENTRYPOINT ["/init_entrypoint.sh"]