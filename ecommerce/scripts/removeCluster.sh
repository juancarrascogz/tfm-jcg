#!/bin/bash
REGION="europe-west2"

PROJECT=$(gcloud info --format='value(config.project)')
FULL_NAME=`gcloud config get-value account`
IFS='@'
read -ra ADDR <<< "$FULL_NAME"
USER_NAME=${ADDR[0]}

echo "Y" | gcloud dataproc clusters delete ${PROJECT}-${USER_NAME}-cluster --region ${REGION}