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

. $WI_HOME/bin/impl/base.sh

# stop if any command fails
set -e

: ${SPARK_SUBMIT?"SPARK_SUBMIT must be set"}
: ${WI_DATA_JAR?"WI_DATA_JAR must be set"}
: ${WI_DATA_DEP_JAR?"WI_DATA_DEP_JAR must be set"}

echo "Kill any previously running webindex Fluo application or Spark job"
$WI_HOME/bin/webindex kill

FLUO_APP=`get_prop fluoApp`
FLUO_CMD=$FLUO_HOME/bin/fluo
if [ ! -f $FLUO_CMD ]; then
  echo "Fluo command script does not exist at $FLUO_CMD"
  exit 1
fi

FLUO_APP_HOME=$FLUO_HOME/apps/$FLUO_APP
if [ -d $FLUO_APP_HOME ]; then
  echo "Deleting existing $FLUO_APP_HOME"
  rm -rf $FLUO_APP_HOME
fi

$FLUO_CMD new $FLUO_APP

FLUO_APP_LIB=$FLUO_APP_HOME/lib
cp $WI_DATA_JAR $FLUO_APP_LIB
mvn package -Pcopy-dependencies -DskipTests -DoutputDirectory=$FLUO_APP_LIB
# Add webindex core and its dependencies
cp $WI_HOME/modules/core/target/webindex-core-$WI_VERSION.jar $FLUO_APP_LIB

$FLUO_CMD exec $FLUO_APP webindex.data.Configure $WI_CONFIG

$FLUO_CMD init $FLUO_APP --force

$SPARK_SUBMIT --class webindex.data.Init $COMMON_SPARK_OPTS \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.executor.extraJavaOptions=-XX:+UseCompressedOops \
    $WI_DATA_DEP_JAR $1

$FLUO_CMD start $FLUO_APP

echo "Webindex init has completed successfully."
