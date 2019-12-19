#!/usr/bin/env bash

set +x

export COMPOSE_CONVERT_WINDOWS_PATHS=0
export MSYS_NO_PATHCONV=1

# set current working directory to project root
cd `dirname "$0"` && cd ..

docker run --rm -d -p 9324:9324 -v ${PWD}/config/elasticmq.conf:/opt/elasticmq.conf --name=sqs\
 softwaremill/elasticmq:latest

#docker run --rm --name sqs -p 9324:9324 -p 9325:9325 -v ${PWD}/config:/opt/custom\
# -d roribio16/alpine-sqs:latest