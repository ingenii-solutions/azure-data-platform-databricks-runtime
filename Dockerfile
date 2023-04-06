ARG REGISTRY
ARG REPOSITORY
ARG VERSION
FROM ${REGISTRY}/${REPOSITORY}:${VERSION}

ARG PACKAGE_VERSION

COPY dist/ingenii_data_engineering-0.3.2-py3-none-any.whl .
RUN /databricks/python3/bin/pip install ingenii_data_engineering-0.3.2-py3-none-any.whl

# Ingenii
COPY dist/ingenii_databricks-${PACKAGE_VERSION}-py3-none-any.whl .
RUN /databricks/python3/bin/pip install ingenii_databricks-${PACKAGE_VERSION}-py3-none-any.whl
