#!/bin/bash -ex

cat << EOF > /etc/redpanda/redpanda.yaml
redpanda:
  auto_create_topics_enabled: true
  default_topic_partitions: 1

EOF

exec "$@"
