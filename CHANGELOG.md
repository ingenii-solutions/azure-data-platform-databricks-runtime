# CHANGELOG

| Version | Changes |
| --- | --- |
| 0.6.3 | **Engineering:** Handle when file table has a subset of columns |
| 0.6.2 | **Engineering:** Boost base Docker image to 9.x |
| 0.6.1 | **Engineering:** Integration tests and minor fixes |
| 0.6.0 | **Engineering:** Models and snapshots created after source update |
| 0.5.2 | **Engineering:** Add 'REPLACE' strategy |
| 0.5.1 | **Engineering:** Utility to abandon bad file ingestions, Create source table metadata if missing |
| 0.5.0 | **dbt:** Upgrade dbt and dbt-spark to version 0.21.1, so no need for custom dbt-spark package. **Engineering:** Stages of the ingestion pipeline can be referred to by ENUMs |
| 0.4.3 | **General:** Added pyodbc dependencies |
| 0.4.2 | **Engineering:** Handle where schema has added columns, and expanding the delta table accordingly |
| 0.4.1 | **Engineering:** Upgrade Ingenii Data Engineering version to 0.2.1 |
| 0.4.0 | **Engineering:** Archive file before any pre-processing to avoid triggering the ingestion twice |
| 0.3.4 | **Engineering:** Handle if ingesting a .csv file and the columns are in a different order to the schema |
| 0.3.3 | **Engineering:** Able to read new-line separated JSON files |
| 0.3.2 | **Engineering:** Call pre-processing scripts |
| 0.3.0 | **Engineering:** Able to create and update configurations for Data Factory to connect with SFTP and API connections, for some authentication methods |
| 0.2.0 | **dbt:** Added schema validation code, custom dbt-spark package |
| 0.1.0 | **General:** Introduced the ingenii_databricks package |
| 0.0.1 | **General:** Provide a stable data build tool (DBT) environment |
