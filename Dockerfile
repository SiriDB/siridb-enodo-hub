FROM python:3.8
# RUN apt-get update && \
#     apt-get upgrade -y && \
#     apt-get install -y git python3-pip gcc python3-dev musl-dev && \
COPY . /opt/siridb-enodo-hub
WORKDIR /opt/siridb-enodo-hub
RUN pip install -r requirements.txt

# Client (Socket) connections
EXPOSE 9103
# Web api and websockets
EXPOSE 80

# Overwrite default configuration parameters
ENV INTERNAL_SOCKET_SERVER_PORT 9103
ENV INTERNAL_SOCKET_SERVER_HOSTNAME 0.0.0.0
ENV BASIC_AUTH_USERNAME enodo
ENV BASIC_AUTH_PASSWORD enodo
ENV LOG_PATH /tmp/enodo/log.log
ENV CLIENT_MAX_TIMEOUT 35
ENV SERIES_SAVE_PATH /tmp/enodo/series
ENV ENODO_BASE_SAVE_PATH /tmp/enodo
ENV SAVE_TO_DISK_INTERVAL 20
ENV ENABLE_REST_API true
ENV ENABLE_SOCKET_IO_API true
ENV DISABLE_SAFE_MODE true
ENV MAX_IN_QUEUE_BEFORE_WARNING 25
ENV MIN_DATA_POINTS 10
ENV WATCHER_INTERVAL 1
ENV SIRIDB_CONNECTION_CHECK_INTERVAL 30
ENV INTERVAL_SCHEDULES_SERIES 3600

CMD ["/opt/siridb-enodo-hub/main.py"]

ENTRYPOINT ["python"]
