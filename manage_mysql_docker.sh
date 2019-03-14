#!/bin/bash

if [ "$#" -lt 1 ]; then
    echo "Usage: ./script.sh [create/destroy]"
    echo "Example: ./script create"
    exit 1
fi

MODE=$1
DOCKER_MYSQL_PORT="3306"

case ${MODE} in
    "create")
    docker pull genschsa/mysql-employees && \
    docker run --rm -d --name pipewrench-mysql -p ${DOCKER_MYSQL_PORT}:${DOCKER_MYSQL_PORT} -e MYSQL_ROOT_PASSWORD=admin -v $PWD/data:/var/lib/mysql genschsa/mysql-employees
    ;;
    "destroy")
    docker kill pipewrench-mysql
    ;;
    *)
    echo "Valid options are: create or destroy"
    exit 1
    ;;
esac