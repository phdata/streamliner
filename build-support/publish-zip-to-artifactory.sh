#!/bin/bash

set -euo
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PARENT=$(dirname ${DIR})
ZIP=${PARENT}/target/universal/streamliner-$(cat ${PARENT}/version).zip

test -f $ZIP

echo "Uploading ${ZIP} to https://repo.phdata.io/public/streamliner/raw/files/streamliner-4.3.zip"
cloudsmith push raw phdata/streamliner $ZIP

