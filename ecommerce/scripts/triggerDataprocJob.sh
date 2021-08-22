#!/bin/bash

REGION="europe-west2"

PROJECT=$(gcloud info --format='value(config.project)')
FULL_NAME=`gcloud config get-value account`
IFS='@'
read -ra ADDR <<< "$FULL_NAME"
USER_NAME=${ADDR[0]}

if [ ! -z $1 ]; then
  CLUSTER_NAME=$1
fi

if [ ! -z $2 ]; then
  JOB_LOCATION=$2
fi

if [ ! -z $3 ]; then
  PARAMETERS=$3
fi

if [ ! -z $4 ]; then
  REGION=$4
fi

gcloud dataproc jobs submit spark \
  --region ${REGION} \
  --cluster ${PROJECT}-${USER_NAME}-cluster \
  --jar ${JOB_LOCATION} \
  --jars "gs://spark-lib/bigquery/spark-bigquery-latest.jar" \
  -- ${PARAMETERS}