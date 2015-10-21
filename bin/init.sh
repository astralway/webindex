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

 . $BIN_DIR/impl/base.sh "$@"

# stop if any command fails
set -e

FLUO_HOME=`get_prop fluoHome`
if [ ! -d $FLUO_HOME ]; then
  echo "Fluo home does not exist at $FLUO_HOME"
  exit 1
fi

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

java -cp $WI_DATA_DEP_JAR io.fluo.webindex.data.PrintProps $WI_HOME/conf/data.yml >> $FLUO_APP_HOME/conf/fluo.properties

$FLUO_CMD init $FLUO_APP --force

spark-submit --class io.fluo.webindex.data.Init \
    --master yarn-client \
    --num-executors `get_prop sparkExecutorInstances` \
    --executor-memory `get_prop sparkExecutorMemory` \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.executor.extraJavaOptions=-XX:+UseCompressedOops \
    $WI_DATA_DEP_JAR $WI_HOME/conf/data.yml

$FLUO_CMD start $FLUO_APP
