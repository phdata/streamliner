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
	$(MAKE) -C ../ package

clean: remove-install
	$(MAKE) -C ../ clean

itest:
	$(MAKE) install-here
	./run-integration-tests.sh

install-here: remove-install package
	unzip ../target/universal/streamliner-$$(cat ../version) -d streamliner
	mv streamliner/streamliner-$$(cat ../version)/* ./streamliner
	rm -rf ./streamliner/streamliner-$$(cat ../version)

remove-install:
	rm -rf ./streamliner