#!/usr/bin/env bash
# Provides default logging for a Makefile target
# Puts stdin/out to `logs` dir when used in target: sh run-with-logging.sh <command> $@
# example:
# sqoop-create: sqoop-create.sh #### Create Sqoop job
#        ./run-with-logging.sh sqoop-create.sh $@

set -euox pipefail

RED='\033[0;31m'
NC='\033[0m'
GREEN='\033[0;32m'
ORAN='\033[0;33m'

timestamp=`date +"%D %T"`

TARGET_NAME=$2

mkdir -p logs
mkdir -p logs/"$(date +"%d-%m-%Y")"

TARGET_LOG=logs/"$(date +"%d-%m-%Y")"/${TARGET_NAME}.log
STATUS_LOG=logs/status.log

"$@" 2>&1 | tee -a $TARGET_LOG

check_b=`echo ${PIPESTATUS[0]}`
table_name=`basename "$PWD"`

if [ $check_b -ne 0 ]
    then
        echo -e "$timestamp|$table_name|${RED}FAIL${NC}: $@ " 2>&1 | tee -a  $STATUS_LOG
        exit 1
    else
        echo -e "$timestamp|$table_name|${GREEN}PASS${NC}: $@" 2>&1 | tee -a   $STATUS_LOG
fi