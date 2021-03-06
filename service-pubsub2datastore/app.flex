runtime: go
service: sub-worker
env: flex

resources:
  cpu: .5
  memory_gb: 1.3
  disk_size_gb: 10

automatic_scaling:
  min_num_instances: 1
  max_num_instances: 2
  cool_down_period_sec: 60
  cpu_utilization:
    target_utilization: 0.75

env_variables:
  GOOGLE_CLOUD_PROJECT: tf-admin-aabm0pul
#  PUBSUB_TOPIC: us.chicago-city.transportation.traffic-tracker-congestion-estimates
  DATASTORE_SERVICE: https://datastore-worker-dot-tf-admin-aabm0pul.appspot.com/entry
  DS_NAMESPACE: NorthAmerica
  DATASET_PARENT_KEY: ahNrfnRmLWFkbWluLWFhYm0wcHVscj0LEgdjYXRhbG9nGICAgICAgIAKDAsSCGNhdGVnb3J5GICAgICA8ogKDAsSB2RhdGFzZXQYgICAgIDXjAoMogEMTm9ydGhBbWVyaWNh
