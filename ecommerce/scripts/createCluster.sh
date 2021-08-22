#!/bin/bash

REGION="europe-west2"

PROJECT=$(gcloud info --format='value(config.project)')
FULL_NAME=`gcloud config get-value account`
IFS='@'
read -ra ADDR <<< "$FULL_NAME"
USER_NAME=${ADDR[0]}


if [ ! -z $1 ]; then
  REGION=$1
fi
ZONE="${REGION}-a"


gcloud dataproc clusters create ${PROJECT}-${USER_NAME}-cluster \
    --scopes pubsub,bigquery,cloud-platform  \
    --image-version 1.4 \
    --master-machine-type n1-standard-2 \
    --master-boot-disk-size 15 \
    --num-workers 2 \
    --worker-machine-type n1-standard-2 \
    --worker-boot-disk-size 15 \
    --region ${REGION} 