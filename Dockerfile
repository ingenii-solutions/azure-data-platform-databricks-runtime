ARG REGISTRY
ARG REPOSITORY
ARG VERSION
FROM ${REGISTRY}/${REPOSITORY}:${VERSION}

ARG PACKAGE_VERSION

# Ingenii
COPY dist/ingenii_databricks-${PACKAGE_VERSION}-py3-none-any.whl .
RUN /databricks/python3/bin/pip install ingenii_databricks-${PACKAGE_VERSION}-py3-none-any.whl
