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

function manage_docker () {
	case $1 in
		"create")
		echo "Creating docker containers"
		docker-compose -f ${DIR}/docker-compose.yml up -d
		echo 'waiting for mysql container to be available...'
		i=0
		while [ ${i} -lt 10 ];do
			docker-compose -f ${DIR}/docker-compose.yml exec -T mysql mysql -h localhost -P 3306 -u root -ppipewrench -e 'show databases;' &> /dev/null
			if [[ "$?" -eq "0" ]];then
				echo 'service ready'
				break;
			fi
			i=$((i+1))
			sleep 30
		done
		;;
		"destroy")
		echo "Removing docker containers"
		docker-compose -f ${DIR}/docker-compose.yml down
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

trap 'manage_docker destroy' EXIT
manage_docker create

set -e
echo "running integration tests"
docker-compose -f ${DIR}/docker-compose.yml exec -T kimpala /run-all-services.sh
docker-compose -f ${DIR}/docker-compose.yml exec -T kimpala hdfs dfs -put /mount/passwordFile /user/root/

for PIPELINE_TEMPLATE in ${DIR}/templates/*; do
	FILE_NAME=${PIPELINE_TEMPLATE##*/}
	BASENAME=${FILE_NAME%.*}
	echo "RUNNING TEMPLATE: ${FILE_NAME}"

	docker-compose -f ${DIR}/docker-compose.yml exec -T kimpala pipewrench schema \
	--config /mount/templates/${FILE_NAME} \
	--database-password pipewrench -Dlogback.configurationFile=conf/logback.xml

	docker-compose -f ${DIR}/docker-compose.yml exec -T kimpala pipewrench scripts \
	--config /output/${BASENAME}/test/conf/pipewrench-configuration.yml \
	--output-path /output/${BASENAME} \
	--template-directory /mount/pipewrench/templates \
	--type-mapping /mount/pipewrench/conf/type-mapping.yml -Dlogback.configurationFile=conf/logback.xml

	docker-compose -f ${DIR}/docker-compose.yml exec -T kimpala chmod 770 -R /output
	docker-compose -f ${DIR}/docker-compose.yml exec -T kimpala make -C /output/${BASENAME}/${BASENAME} first-run-all
	docker-compose -f ${DIR}/docker-compose.yml exec -T kimpala make -C /output/${BASENAME}/${BASENAME} clean-all
done
