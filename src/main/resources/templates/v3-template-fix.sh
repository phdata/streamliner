#!/usr/bin/env bash

find . -type f -name "*.ssp" -print0 | xargs -0 sed -i '' -e 's/configuration.hadoop./configuration.hadoop.get./g'

find . -type f -name "*.ssp" -print0 | xargs -0 sed -i '' -e 's/Database.path/Database.path.get/g'#!/usr/bin/env bash