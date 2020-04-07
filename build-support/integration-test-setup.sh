#!/bin/bash

set -e
export SBT_OPTS="-Xmx1536M -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M"

echo "Installing Docker Compose"
curl -L "https://github.com/docker/compose/releases/download/1.21.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
sbt universal:packageBin
unzip target/universal/streamliner-$(cat ./version) -d integration-tests/streamliner
mv integration-tests/streamliner/streamliner-$(cat ./version)/* integration-tests/streamliner