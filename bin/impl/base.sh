#!/bin/bash

# Copyright 2015 Webindex authors (see AUTHORS)
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
: ${WI_CONFIG?"WI_CONFIG must be set"}
: ${SPARK_HOME?"SPARK_HOME must be set"}

function get_prop {
  echo "`grep $1 $WI_CONFIG | cut -d ' ' -f 2`"
}

: ${HADOOP_CONF_DIR?"HADOOP_CONF_DIR must be set in bash env or conf/webindex-env.sh"}
if [ ! -d $HADOOP_CONF_DIR ]; then
  echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR does not exist"
  exit 1
fi
: ${FLUO_HOME?"FLUO_HOME must be set in bash env or conf/webindex-env.sh"}
if [ ! -d $FLUO_HOME ]; then
  echo "FLUO_HOME=$FLUO_HOME does not exist"
  exit 1
fi

: ${WI_EXECUTOR_INSTANCES?"WI_EXECUTOR_INSTANCES must be set in bash env or conf/webindex-env.sh"}
: ${WI_EXECUTOR_MEMORY?"WI_EXECUTOR_MEMORY must be set in bash env or conf/webindex-env.sh"}
export COMMON_SPARK_OPTS="--master yarn-client --num-executors $WI_EXECUTOR_INSTANCES --executor-memory $WI_EXECUTOR_MEMORY"

export SPARK_SUBMIT=$SPARK_HOME/bin/spark-submit
if [ ! -f $SPARK_SUBMIT ]; then
  echo "The spark-submit command cannot be found in SPARK_HOME=$SPARK_HOME.  Please set SPARK_HOME in conf/webindex-env.sh"
  exit 1
fi

hash mvn 2>/dev/null || { echo >&2 "Maven must be installed & mvn command must be on path.  Aborting."; exit 1; }

# Stop if any command after this fails
set -e

export WI_DATA_JAR=$WI_HOME/modules/data/target/webindex-data-$WI_VERSION.jar
export WI_DATA_DEP_JAR=$WI_HOME/modules/data/target/webindex-data-$WI_VERSION-shaded.jar
if [ ! -f $WI_DATA_DEP_JAR ]; then
  echo "Building $WI_DATA_DEP_JAR"
  cd $WI_HOME

  : ${ACCUMULO_VERSION?"ACCUMULO_VERSION must be set in bash env or conf/webindex-env.sh"}
  : ${FLUO_VERSION?"FLUO_VERSION must be set in bash env or conf/webindex-env.sh"}
  : ${THRIFT_VERSION?"THRIFT_VERSION must be set in bash env or conf/webindex-env.sh"}
  mvn clean package -Pcreate-shade-jar -DskipTests -Dfluo.version=$FLUO_VERSION -Daccumulo.version=$ACCUMULO_VERSION -Dthrift.version=$THRIFT_VERSION
fi
