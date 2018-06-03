#!/bin/bash
CONTROLS_TOPIC_NAME="common.controls"
SUB_PULL_CONTROLS="pull-common.controls"
SUB_PUSH2CASSANDRA_CONTROLS="push-to-cassandra.common.controls"

SESSIONS_TOPIC_NAME="common.sessions"
SUB_PUSH2CASSANDRA_SESSIONS="push-to-cassandra.common.sessions"

TRAFFIC_TRACKER_TOPIC_NAME="us.chicago-city.transportation.traffic-tracker-congestion-estimates"
SUB_PUSH2CASSANDRA="push-to-cassandra.us.chicago-city.transportation.traffic-tracker-congestion-estimates"

TRAFFIC_TRACKER2018CURRENT_TOPIC_NAME="us.chicago-city.transportation.traffic-tracker-2018-current"
TRAFFIC_TRACKER2018CURRENT_PUSH2CASSANDRA="push-to-cassandra.us.chicago-city.transportation.traffic-tracker-2018-current"

# TODO: check if exist
gcloud alpha pubsub topics create ${TRAFFIC_TRACKER_TOPIC_NAME}

# TODO: check if subscription exist and remove it
gcloud alpha pubsub subscriptions create ${SUB_PUSH2DATASTORE} \
     --topic ${TRAFFIC_TRACKER_TOPIC_NAME} \
     --push-endpoint https://push-subscription-worker-dot-tf-admin-aabm0pul.appspot.com/push/cassandra \
     --ack-deadline 30

# TODO: check if exist
gcloud alpha pubsub topics create ${SESSIONS_TOPIC_NAME}
gcloud alpha pubsub subscriptions create ${SUB_PUSH2CASSANDRA_SESSIONS} \
     --topic ${SESSIONS_TOPIC_NAME} \
     --push-endpoint https://push-subscription-worker-dot-tf-admin-aabm0pul.appspot.com/push/${SESSIONS_TOPIC_NAME}/cassandra \
     --ack-deadline 30


#
gcloud alpha pubsub topics create ${TRAFFIC_TRACKER2018CURRENT_TOPIC_NAME}
gcloud alpha pubsub subscriptions create ${TRAFFIC_TRACKER2018CURRENT_PUSH2CASSANDRA} \
     --topic ${TRAFFIC_TRACKER2018CURRENT_TOPIC_NAME} \
     --push-endpoint https://push-subscription-worker-dot-tf-admin-aabm0pul.appspot.com/push/${TRAFFIC_TRACKER2018CURRENT_TOPIC_NAME}/cassandra \
     --ack-deadline 30



#
gcloud alpha pubsub topics create ${CONTROLS_TOPIC_NAME}

gcloud alpha pubsub subscriptions create ${SUB_PUSH2CASSANDRA_CONTROLS} \
     --topic ${CONTROLS_TOPIC_NAME} \
     --push-endpoint https://push-subscription-worker-dot-tf-admin-aabm0pul.appspot.com/push/${CONTROLS_TOPIC_NAME}/cassandra \
     --ack-deadline 30

gcloud alpha pubsub subscriptions create ${SUB_PULL_CONTROLS} --topic=${CONTROLS_TOPIC_NAME}
