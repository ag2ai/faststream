#!/bin/bash

if test -z "$AG2_PROJECT"; then
      echo 'AG2_PROJECT variable not set, setting to current directory'
      export AG2_PROJECT=`pwd`
fi
echo AG2_PROJECT variable set to $AG2_PROJECT

export UID=$(id -u)
export GID=$(id -g)

export DOCKER_COMPOSE_PROJECT="${USER//./_}-faststream"
echo DOCKER_COMPOSE_PROJECT variable set to $DOCKER_COMPOSE_PROJECT
export KAFKA_HOSTNAME="${DOCKER_COMPOSE_PROJECT}-kafka-1"
echo KAFKA_HOSTNAME variable set to $KAFKA_HOSTNAME
export PRESERVE_ENVS="KAFKA_HOSTNAME,KAFKA_PORT"
