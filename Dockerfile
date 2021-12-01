FROM databricksruntime/standard:7.x

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV PYTHONUNBUFFERED 1

ARG PACKAGE_VERSION
ARG DBT_SPARK_VERSION

RUN apt-get update && \
        apt-get install -yq libsasl2-dev build-essential g++ unixodbc-dev && \
        apt-get clean

WORKDIR /app

COPY requirements.txt requirements.txt
RUN /databricks/conda/envs/dcs-minimal/bin/pip install -r requirements.txt

# data build tool (DBT)
RUN ln -s /databricks/conda/envs/dcs-minimal/bin/dbt /usr/local/bin/dbt

# Ingenii
COPY dist/ingenii_databricks-${PACKAGE_VERSION}-py3-none-any.whl .
RUN /databricks/conda/envs/dcs-minimal/bin/pip install ingenii_databricks-${PACKAGE_VERSION}-py3-none-any.whl
