#!/bin/bash

# Launches a dockerized kafka cluster configured for testing with KafkaEx
#
# This script attempts to auto-detect the ip address of an active network
# interface using `./scripts/active_ifaces.sh`.  You can override this by
# supplying the name of an interface through the IP_IFACE env var - e.g.,
#
# IP_IFACE=eth0 ./scripts/docker_up.sh
#
# This script should be run from the project root

set -e

# Kafka needs to know our ip address so that it can advertise valid
# connection details
if [ -z ${IP_IFACE} ]
then
  echo Detecting active network interface
  IP_IFACE=$(ifconfig | ./scripts/active_ifaces.sh | head -n 1 | cut -d ':' -f1)
fi

export DOCKER_IP=$(ifconfig ${IP_IFACE} | grep 'inet ' | awk '{print $2}' | cut -d ':' -f2)

# for debugging purposes
echo Detected active network interface ${IP_IFACE} with ip ${DOCKER_IP}

docker-compose up -d

# create topics needed for testing
docker-compose exec kafka3 bash -lc '
kafka-topics --bootstrap-server kafka3:19094 --create --if-not-exists --topic consumer_group_implementation_test --partitions 4 --replication-factor 2
kafka-topics --bootstrap-server kafka3:19094 --create --if-not-exists --topic test0p8p0 --partitions 4 --replication-factor 2
'
