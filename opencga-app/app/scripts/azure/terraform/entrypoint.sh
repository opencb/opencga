#!/bin/bash
set -e

echo "---"
echo " Logging in with Managed Service Identity"
echo "---"
az login --identity -u "${ARM_IDENTITY}"

echo "---"
echo "Create Azure Blob Storage container if it doesn't exist"
echo "---"
az storage container create --name "${TF_VAR_state_storage_container_name}" --account-name "${TF_VAR_state_storage_account_name}" --account-key "${TF_VAR_state_storage_account_key}"

export ARM_ACCESS_KEY="${TF_VAR_state_storage_account_key}"

echo "---"
echo "Initializing terraform"
echo "---"
terraform init \
    -backend-config "storage_account_name=$TF_VAR_state_storage_account_name" \
    -backend-config "container_name=$TF_VAR_state_storage_container_name" \
    -backend-config "key=$TF_VAR_state_storage_blob_name"

echo "---"
echo "Applying terraform"
echo "---"
terraform apply -auto-approve