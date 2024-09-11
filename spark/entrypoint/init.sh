#!/bin/bash

# Set permissions
chmod 777 /opt/spark/.ivy2 /tmp/spark-events /var/data /opt

# Run the original entrypoint script
exec /opt/entrypoint.sh "$@"