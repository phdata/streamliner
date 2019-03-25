#!/bin/bash

set -e
export SBT_OPTS="-Xmx1536M -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"

echo "Installing Docker Compose"
curl -L "https://github.com/docker/compose/releases/download/1.21.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
sbt universal:packageBin
unzip target/universal/pipewrench-$(cat ./version) -d integration-tests/pipewrench
mv integration-tests/pipewrench/pipewrench-$(cat ./version)/* integration-tests/pipewrench