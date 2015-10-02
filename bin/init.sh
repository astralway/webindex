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

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
WI_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

DATA_CONFIG=$WI_HOME/conf/data.yml
if [ ! -f $DATA_CONFIG ]; then
  echo "You must create $DATA_CONFIG"
  exit 1
fi

function get_prop {
  VALUE="`grep $1 $DATA_CONFIG | cut -d ' ' -f 2`"
  if [ -z $VALUE ]; then
    echo "The property $1 must be set data.yml"
    exit 1
  fi
  echo $VALUE
}

BUILD=false
if [ ! -z $1 ]; then
  if [ "$1" == "--build" ]; then
    BUILD=true
  else
    echo "Unknown argument $1"
    exit 1
  fi
fi

hash spark-submit 2>/dev/null || { echo >&2 "Spark must be installed & spark-submit command must be on path.  Aborting."; exit 1; }
hash mvn 2>/dev/null || { echo >&2 "Maven must be installed & mvn command must be on path.  Aborting."; exit 1; }

export HADOOP_CONF_DIR=`get_prop hadoopConfDir`

# stop if any command fails
set -e

WI_DATA_JAR=$WI_HOME/modules/data/target/webindex-data-0.0.1-SNAPSHOT.jar
if [ "$BUILD" = true -o ! -f $WI_DATA_JAR ]; then
  echo "Building $WI_DATA_JAR"
  cd $WI_HOME
  mvn clean install -DskipTests
fi
WI_DATA_DEP_JAR=$WI_HOME/modules/data/target/webindex-data-0.0.1-SNAPSHOT-jar-with-dependencies.jar
if [ "$BUILD" = true -o ! -f $WI_DATA_DEP_JAR ]; then
  echo "Building $WI_DATA_DEP_JAR"
  cd $WI_HOME/modules/data
  mvn package assembly:single -DskipTests
fi

NUM_EXECUTORS=`get_prop sparkExecutorInstances`
EXECUTOR_MEMORY=`get_prop sparkExecutorMemory`
FLUO_HOME=`get_prop fluoHome`
if [ ! -d $FLUO_HOME ]; then
  echo "Fluo home does not exist at $FLUO_HOME"
  exit 1
fi

command -v spark-submit >/dev/null 2>&1 || { echo >&2 "The 'spark-submit' command must be available on PATH.  Aborting."; exit 1; }

FLUO_APP=`get_prop fluoApp`
FLUO_CMD=$FLUO_HOME/bin/fluo
$FLUO_CMD stop $FLUO_APP || true

FLUO_APP_HOME=$FLUO_HOME/apps/$FLUO_APP
if [ -d $FLUO_APP_HOME ]; then
  echo "Deleting existing $FLUO_APP_HOME"
  rm -rf $FLUO_APP_HOME
fi

$FLUO_CMD new $FLUO_APP

FLUO_APP_LIB=$FLUO_APP_HOME/lib
cp $WI_DATA_JAR $FLUO_APP_LIB
mvn dependency:get -Dartifact=io.fluo:fluo-recipes-core:1.0.0-beta-1-SNAPSHOT:jar -Ddest=$FLUO_APP_LIB
mvn dependency:get -Dartifact=io.fluo:fluo-recipes-accumulo:1.0.0-beta-1-SNAPSHOT:jar -Ddest=$FLUO_APP_LIB

APP_PROPS=$FLUO_APP_HOME/conf/fluo.properties
java -cp $WI_DATA_DEP_JAR io.fluo.webindex.data.PrintProps $WI_HOME/conf/data.yml >> $APP_PROPS

$FLUO_CMD init $FLUO_APP --force

spark-submit --class io.fluo.webindex.data.Init \
    --master yarn-client \
    --num-executors $NUM_EXECUTORS \
    --executor-memory $EXECUTOR_MEMORY \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.executor.extraJavaOptions=-XX:+UseCompressedOops \
    $WI_DATA_DEP_JAR $WI_HOME/conf/data.yml

$FLUO_CMD start $FLUO_APP
