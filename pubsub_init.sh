#!/bin/bash

TOPIC_NAME="us.chicago-city.transportation.traffic-tracker-congestion-estimates"
SUB_PUSH2DATASTORE="push-to-datastore"

# check if exist
gcloud alpha pubsub topics create ${TOPIC_NAME}

# TODO: check if subscription exist and remove it

gcloud alpha pubsub subscriptions create ${SUB_PUSH2DATASTORE} \
     --topic ${TOPIC_NAME} \
     --push-endpoint https://save2datastore-worker-dot-tf-admin-aabm0pul.appspot.com/push \
     --ack-deadline 30

# create subscription to pull
