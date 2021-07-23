#!/bin/bash
set -euo
set -x
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PARENT=$(dirname ${DIR})
VERSION=$(cat ${PARENT}/version)
ZIP=${PARENT}/target/universal/streamliner-$VERSION.zip

test -f $ZIP

echo "Uploading ${ZIP}"
cloudsmith push raw phdata/streamliner $ZIP
cloudsmith push maven --artifact-id streamliner --group-id io.phdata.streamliner --version $VERSION phdata/streamliner --packaging zip $ZIP

