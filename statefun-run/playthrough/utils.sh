#!/bin/bash

# Sends messages to Kafka within docker-compose setup.
# Parameters:
#  - param1: message key
#  - param2: message payload
#  - param3: Kafka topic
send_to_kafka () {
    local key=$1
    local payload=$2
    local topic=$3
    echo "Sending \"$payload\" with key \"$key\" to \"$topic\" topic"
    echo "$key: $payload" | /opt/kafka_2.12-2.8.0/bin/kafka-console-producer.sh --topic $topic --broker-list localhost:9092 --property 'parse.key=true' --property 'key.separator=:'
}