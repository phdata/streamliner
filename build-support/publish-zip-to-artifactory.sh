#!/bin/bash

set -euo
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PARENT=$(dirname ${DIR})
ZIP=${PARENT}/target/universal/streamliner-$(cat ${PARENT}/version).zip
source ${DIR}/artifactory.env

test -f $ZIP

echo "Uploading ${ZIP} to https://repository.phdata.io/artifactory/binary/phdata/streamliner/"
curl -u${ARTIFACTORY_USER}:${ARTIFACTORY_TOKEN} -T $ZIP "https://repository.phdata.io/artifactory/binary/phdata/streamliner/"

