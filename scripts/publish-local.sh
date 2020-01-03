#!/bin/bash -l
set -e

imageName=s3mock-dev
imageId=$(docker images */${imageName} --format '{{.ID}}')
if [[ "${imageId}" != '' ]]; then
    echo "Deleting image ${imageName}"
    docker rmi "${imageId}"
fi

SBT_OPTS="-DdevVersion=1.0.0-SNAPSHOT" sbt clean compile docker:publishLocal
