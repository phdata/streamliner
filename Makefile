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

package:
	sbt universal:packageBin

clean: remove-install
	sbt clean

test:
	sbt test

itest:
	integration-tests/run-integration-tests.sh create
	(sbt it:test && integration-tests/run-integration-tests.sh destroy) || integration-tests/run-integration-tests.sh destroy
	$(MAKE) -C integration-tests/ itest

install-here: remove-install
	unzip target/universal/pipewrench-$$(cat version)

remove-install:
	rm -rf pipewrench-$$(cat version)

publish: clean package
	build-support/publish-zip-to-artifactory.sh
