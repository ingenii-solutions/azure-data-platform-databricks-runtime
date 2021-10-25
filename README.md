# Ingenii Databricks Platform

[![Maintainer](https://img.shields.io/badge/maintainer%20-ingenii-orange?style=flat)](https://ingenii.dev/)
[![License](https://img.shields.io/badge/license%20-MPL2.0-orange?style=flat)](https://github.com/ingenii-solutions/terraform-azurerm-key-vault/blob/main/LICENSE)
[![Contributing](https://img.shields.io/badge/howto%20-contribute-blue?style=flat)](https://github.com/ingenii-solutions/data-platform-databricks-runtime/blob/main/CONTRIBUTING.md)

## Details
* Base image: [databricksruntime/standard:7.x](https://hub.docker.com/layers/databricksruntime/standard/7.x/images/sha256-0d51d36c7b927858757fdc828c6a9fd5375b98ffcb186324060d0b334f5149d3?context=explore)
* Registry: ingeniisolutions
* Repository: databricks-runtime
* Current Version: 0.4.2

## Overview

This image is used with Databricks' [Container Services](https://docs.databricks.com/clusters/custom-containers.html) to customise the cluster runtime in the engineering cluster of in the [Ingenii Data Platform](https://ingenii.dev/). This contains an installation of [dbt](https://www.getdbt.com/) and [Ingenii's python package for data engineering](https://github.com/ingenii-solutions/azure-data-platform-data-engineering).

## Data Pipeline Overview

For an overview of the data pipeline and the stages it goes through, please refer to the [Data Pipeline documentation](docs/user/DATAPIPELINE.md)

## dbt Integration

For reading files and testing data we use [dbt](https://www.getdbt.com/) as a framework. For an explanation on how we use dbt and how to set up your own data sources, please refer to the [Ingenii Data Engineering Example repository](https://github.com/ingenii-solutions/azure-data-platform-data-engineering-example).

## dbt-spark

As part of our development, we discovered an issue in [dbt-spark](https://github.com/dbt-labs/dbt-spark) around retrying when connections fail. At time of writing we have [opened a pull request for this fix](https://github.com/dbt-labs/dbt-spark/pull/194), but it is not yet merged. In the meantime, we contain in this repository our own version of dbt-spark (version 0.19.1) in the `dbt-spark` folder, which we install as part of the container runtime creation.

If we upgrade the version of dbt used in the container while this fix is not merged we will have to upgrade our version of dbt-spark to match, which will involve updating the dbt-spark folder and re-applying our fix.

## Versions Matrix

| Image Version | Databricks Runtime | Added Packages |
| --- | --- | --- |
| 0.4.3 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.4.2 <br> ingenii_data_engineering = 0.2.1 |
| 0.4.2 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.4.2 <br> ingenii_data_engineering = 0.2.1 |
| 0.4.1 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.4.1 <br> ingenii_data_engineering = 0.2.1 |
| 0.4.0 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.4.0 <br> ingenii_data_engineering = 0.2.0 |
| 0.3.4 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.4 <br> ingenii_data_engineering = 0.1.5 |
| 0.3.3 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.3 <br> ingenii_data_engineering = 0.1.4 |
| 0.3.2 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.2 <br> ingenii_data_engineering = 0.1.3 |
| 0.3.1 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.1 <br> ingenii_data_engineering = 0.1.0 |
| 0.3.0 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.3.0 |
| 0.2.0 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 (custom) <br> ingenii_databricks = 0.2.0 |
| 0.1.0 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 <br> ingenii_databricks = 0.1.0 |
| 0.0.1 | 7.x | dbt_core = 0.19.1 <br> dbt_pyspark = 0.19.1 |
