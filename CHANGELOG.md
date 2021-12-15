# CHANGELOG

## 0.5.1

NEW FEATURES:
- **Engineering:** Utility to abandon bad file ingestions

## 0.5.0

IMPROVEMENTS:
- **dbt:** Upgrade dbt and dbt-spark to version 0.21.1, so no need for custom dbt-spark package
- **Engineering:** Stages of the ingestion pipeline can be referred to by ENUMs

## 0.4.3

NEW FEATURES:
- **General:** Added pyodbc dependencies

## 0.4.2

IMPROVEMENTS:
- **Engineering:** Handle where schema has added columns, and expanding the delta table accordingly

## 0.4.1

IMPROVEMENTS:
- **Engineering:** Upgrade Ingenii Data Engineering version to 0.2.1

## 0.4.0

IMPROVEMENTS:
- **Engineering:** Archive file before any pre-processing to avoid triggering the ingestion twice

## 0.3.4

NEW FEATURES:
- **Engineering:** Handle if ingesting a .csv file and the columns are in a different order to the schema

## 0.3.3

NEW FEATURES:
- **Engineering:** Able to read new-line separated JSON files

## 0.3.2

NEW FEATURES:
- **Engineering:** Call pre-processing scripts

## 0.3.0

NEW FEATURES:
- **Engineering:** Able to create and update configurations for Data Factory to connect with SFTP and API connections, for some authentication methods

## 0.2.0

NEW FEATURES:
- **dbt:** Added schema validation code, custom dbt-spark package

## 0.1.0

NEW FEATURES:
- **General:** Introduced the ingenii_databricks package

## 0.0.1

NEW FEATURES:
- **General:** Provide a stable data build tool (DBT) environment.
