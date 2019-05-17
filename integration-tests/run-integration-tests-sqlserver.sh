#!/bin/bash
# Copyright 2018 phData Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $DIR

RDBMS="sqlserver"
DOCKER_COMPOSE="docker-compose -f ${DIR}/docker-compose.yml -f ${DIR}/docker-compose-${RDBMS}.yml"

function manage_docker () {
	case $1 in
		"create")
		echo "Creating docker containers"
		$DOCKER_COMPOSE up -d
		echo 'waiting for MS SQL Server container to be available...'
		$DOCKER_COMPOSE exec -T db await.sh &> /dev/null
		echo 'service ready'
		;;
		"destroy")
		echo "Removing docker containers"
		$DOCKER_COMPOSE down
		;;
		*)
		echo "Valid options are: create or destroy"
		exit 1
		;;
	esac
}

if [ "$#" -eq 1 ]; then
	manage_docker $1
	exit $?
fi

#trap 'manage_docker destroy' EXIT
manage_docker create

set -e
echo "running integration tests"
$DOCKER_COMPOSE exec -T kimpala /run-all-services.sh
$DOCKER_COMPOSE exec -T kimpala /mount/install-jdbc-drivers.sh
$DOCKER_COMPOSE exec -T kimpala hdfs dfs -put /mount/passwordFile /user/root/

for PIPELINE_TEMPLATE in ${DIR}/docker-mount/templates/${RDBMS}/*.yml; do
	FILE_NAME=${PIPELINE_TEMPLATE##*/}
	BASENAME=${FILE_NAME%.*}
	echo "RUNNING TEMPLATE: ${FILE_NAME}"

	$DOCKER_COMPOSE exec -T kimpala pipewrench schema \
	--config /mount/templates/${RDBMS}/${FILE_NAME} \
	--database-password pipewrench

	$DOCKER_COMPOSE exec -T kimpala pipewrench scripts \
	--config /output/${BASENAME}/pipewrench-configuration.yml \
	--output-path /output/${BASENAME} \
	--template-directory /mount/pipewrench/templates \
	--type-mapping /mount/pipewrench/conf/type-mapping.yml

	$DOCKER_COMPOSE exec -T kimpala chmod 770 -R /output
	$DOCKER_COMPOSE exec -T kimpala make -C /output/${BASENAME}/${BASENAME} first-run-all
	$DOCKER_COMPOSE exec -T kimpala make -C /output/${BASENAME}/${BASENAME} clean-all
done

