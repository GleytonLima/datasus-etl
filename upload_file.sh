#!/bin/sh

az storage blob upload \
  --account-name $1 \
  --account-key $2 \
  --container-name $3 \
  --file $4 \
  --name $5
