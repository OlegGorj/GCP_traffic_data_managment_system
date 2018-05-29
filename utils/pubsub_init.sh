#!/bin/bash
CONTROLS_TOPIC_NAME="common.controls"
SUB_PULL_CONTROLS="pull-common.controls"

SESSIONS_TOPIC_NAME="common.sessions"
SUB_PUSH2CASSANDRA_SESSIONS="push-to-cassandra.common.sessions"

TRAFFIC_TRACKER_TOPIC_NAME="us.chicago-city.transportation.traffic-tracker-congestion-estimates"
SUB_PUSH2CASSANDRA="push-to-cassandra.us.chicago-city.transportation.traffic-tracker-congestion-estimates"

# TODO: check if exist
gcloud alpha pubsub topics create ${TRAFFIC_TRACKER_TOPIC_NAME}

# TODO: check if exist
gcloud alpha pubsub topics create ${SESSIONS_TOPIC_NAME}

gcloud alpha pubsub topics create ${CONTROLS_TOPIC_NAME}

# TODO: check if subscription exist and remove it

gcloud alpha pubsub subscriptions create ${SUB_PUSH2DATASTORE} \
     --topic ${TRAFFIC_TRACKER_TOPIC_NAME} \
     --push-endpoint https://push-subscription-worker-dot-tf-admin-aabm0pul.appspot.com/push/cassandra \
     --ack-deadline 30

gcloud alpha pubsub subscriptions create ${SUB_PUSH2CASSANDRA_SESSIONS} \
     --topic ${SESSIONS_TOPIC_NAME} \
     --push-endpoint https://push-subscription-worker-dot-tf-admin-aabm0pul.appspot.com/push/cassandra \
     --ack-deadline 30


gcloud alpha pubsub subscriptions create ${SUB_PULL_CONTROLS} --topic=${CONTROLS_TOPIC_NAME}
