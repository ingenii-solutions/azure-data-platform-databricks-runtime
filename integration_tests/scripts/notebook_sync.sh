#!/bin/sh

az login --service-principal -t ${ARM_TENANT_ID} -u ${ARM_CLIENT_ID} -p ${ARM_CLIENT_SECRET} > /dev/null

DATABRICKS_AAD_TOKEN=$(az account get-access-token  --subscription ${ARM_SUBSCRIPTION_ID} --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d | jq '.accessToken' --raw-output)

DATABRICKS_AAD_TOKEN=${DATABRICKS_AAD_TOKEN} databricks configure --aad-token --host https://${DATABRICKS_HOST}
databricks workspace mkdirs '/Shared/Testing'
ls integration_tests/notebooks | awk -F '.' '{ print $1 }' | xargs -I {} databricks workspace import -l PYTHON integration_tests/notebooks/{}.py '/Shared/Testing/{}' --overwrite
