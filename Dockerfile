FROM databricksruntime/standard:9.x

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV PYTHONUNBUFFERED 1

ARG PACKAGE_VERSION

RUN apt-get update && \
        apt-get install -yq libsasl2-dev build-essential g++ unixodbc-dev && \
        apt-get clean
RUN apt-get install -y libpython3.8-dev

WORKDIR /app

COPY requirements.txt requirements.txt
RUN /databricks/python3/bin/pip install -r requirements.txt

# data build tool (DBT)
RUN ln -s /databricks/conda/envs/dcs-minimal/bin/dbt /usr/local/bin/dbt

# Ingenii
COPY dist/ingenii_databricks-${PACKAGE_VERSION}-py3-none-any.whl .
RUN /databricks/python3/bin/pip install ingenii_databricks-${PACKAGE_VERSION}-py3-none-any.whl
