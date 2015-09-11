#!/bin/bash

# Copyright: 2015 Fluo authors (see AUTHORS)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
WI_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

if [ ! -f $WI_HOME/conf/dropwizard.yml ]; then
  echo "Need to create dropwizard.yml in conf/" 
  exit 1
fi

cd $WI_HOME/modules/ui
mvn clean package

java -jar $WI_HOME/modules/ui/target/webindex-ui-0.0.1-SNAPSHOT.jar server $WI_HOME/conf/dropwizard.yml
