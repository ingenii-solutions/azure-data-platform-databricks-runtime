#!/bin/sh

az login --service-principal -t ${ARM_TENANT_ID} -u ${ARM_CLIENT_ID} -p ${ARM_CLIENT_SECRET} > /dev/null

DATA_LAKE_KEY=$(az storage account keys list --account-name $DATA_LAKE_NAME --query '[0].value' -o tsv)

az storage blob sync --account-name $DATA_LAKE_NAME --account-key $DATA_LAKE_KEY -c dbt -s integration_tests/dbt --only-show-errors

az storage blob sync --account-name $DATA_LAKE_NAME --account-key $DATA_LAKE_KEY -c raw -s integration_tests/data -d test_data --only-show-errors
