# Ingenii Databricks Platform

[![Maintainer](https://img.shields.io/badge/maintainer%20-ingenii-orange?style=flat)](https://ingenii.dev/)
[![License](https://img.shields.io/badge/license%20-MPL2.0-orange?style=flat)](https://github.com/ingenii-solutions/terraform-azurerm-key-vault/blob/main/LICENSE)
[![Contributing](https://img.shields.io/badge/howto%20-contribute-blue?style=flat)](https://github.com/ingenii-solutions/data-platform-databricks-runtime/blob/main/CONTRIBUTING.md)

## Details
* Base image: [databricksruntime/standard:9.x](https://hub.docker.com/layers/databricksruntime/standard/9.x/images/sha256-cb414c7ab3c18e529b5e9cada0af996d8912ea7c3ea8087c68da0bb2768c03ab?context=explore)
* Registry: ingeniisolutions
* Repository: databricks-runtime
* Version: 0.7.0

### Intermediate Images
* Base OS Repository: databricks-runtime-base-os
* Base OS Version: 0.2.0
* Base Python Repository: databricks-runtime-base-python
* Base Python Version: 0.2.0

## Overview

This image is used with Databricks' [Container Services](https://docs.databricks.com/clusters/custom-containers.html) to customise the cluster runtime in the engineering cluster of in the [Ingenii Data Platform](https://ingenii.dev/). This contains an installation of [dbt](https://www.getdbt.com/) and [Ingenii's python package for data engineering](https://github.com/ingenii-solutions/azure-data-platform-data-engineering).

## Data Pipeline Overview

For an overview of the data pipeline and the stages it goes through, please refer to the [Data Pipeline documentation](docs/user/DATAPIPELINE.md)

## dbt Integration

For reading files and testing data we use [dbt](https://www.getdbt.com/) as a framework. For an explanation on how we use dbt and how to set up your own data sources, please refer to the [Ingenii Data Engineering Example repository](https://github.com/ingenii-solutions/azure-data-platform-data-engineering-example).

## Contributions

- [dbt-spark](https://github.com/dbt-labs/dbt-spark) - [retrying when any connections fail](https://github.com/dbt-labs/dbt-spark/pull/194)
