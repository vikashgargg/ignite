FROM python:3.14-slim

ARG PYZELOX_VERSION
ARG PYSPARK_VERSION=4.1.1

RUN python3 -m pip install --no-cache-dir "pyzelox==${PYZELOX_VERSION}"
RUN python3 -m pip install --no-cache-dir "pyspark-client==${PYSPARK_VERSION}"

ENTRYPOINT ["/usr/local/bin/zelox"]
