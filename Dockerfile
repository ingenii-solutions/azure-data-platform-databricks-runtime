FROM databricksruntime/standard:7.x

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV PYTHONUNBUFFERED 1

ARG PACKAGE_VERSION
ARG DBT_SPARK_VERSION

RUN apt-get update && \
        apt-get install -yq libsasl2-dev build-essential && \
        apt-get clean

WORKDIR /app

COPY requirements.txt requirements.txt
RUN /databricks/conda/envs/dcs-minimal/bin/pip install -r requirements.txt

# data build tool (DBT)
RUN ln -s /databricks/conda/envs/dcs-minimal/bin/dbt /usr/local/bin/dbt

# Requirements as we have our own dbt-spark
RUN /databricks/conda/envs/dcs-minimal/bin/pip install "PyHive[hive]>=0.6.0,<0.7.0"
RUN /databricks/conda/envs/dcs-minimal/bin/pip install "thrift>=0.11.0,<0.12.0"

# Our dbt-spark
COPY packages/dbt_spark-$DBT_SPARK_VERSION-py3-none-any.whl .
RUN /databricks/conda/envs/dcs-minimal/bin/pip install dbt_spark-$DBT_SPARK_VERSION-py3-none-any.whl

# Ingenii
COPY dist/ingenii_databricks-${PACKAGE_VERSION}-py3-none-any.whl .
RUN /databricks/conda/envs/dcs-minimal/bin/pip install ingenii_databricks-${PACKAGE_VERSION}-py3-none-any.whl
COPY packages/ingenii_data_engineering-0.1.5-py3-none-any.whl .
RUN /databricks/conda/envs/dcs-minimal/bin/pip install ingenii_data_engineering-0.1.5-py3-none-any.whl
