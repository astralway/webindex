#!/bin/bash

# Copyright 2015 Fluo authors (see AUTHORS)
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

: ${WI_HOME?"WI_HOME must be set"}
: ${DATA_CONFIG?"DATA_CONFIG must be set"}
: ${SPARK_HOME?"SPARK_HOME must be set"}

function get_prop {
  echo "`grep $1 $DATA_CONFIG | cut -d ' ' -f 2`"
}

export SPARK_SUBMIT=$SPARK_HOME/bin/spark-submit
if [ ! -f $SPARK_SUBMIT ]; then
  echo "The spark-submit command cannot be found in SPARK_HOME=$SPARK_HOME.  Please set SPARK_HOME in conf/webindex-env.sh"
  exit 1
fi

hash mvn 2>/dev/null || { echo >&2 "Maven must be installed & mvn command must be on path.  Aborting."; exit 1; }

# Stop if any command after this fails
set -e

export WI_DATA_JAR=$WI_HOME/modules/data/target/webindex-data-0.0.1-SNAPSHOT.jar
export WI_DATA_DEP_JAR=$WI_HOME/modules/data/target/webindex-data-0.0.1-SNAPSHOT-jar-with-dependencies.jar
if [ ! -f $WI_DATA_DEP_JAR ]; then
  echo "Installing all webindex jars"
  cd $WI_HOME
  mvn clean install -DskipTests
  echo "Building $WI_DATA_DEP_JAR"
  cd $WI_HOME/modules/data
  mvn package assembly:single -DskipTests
fi
