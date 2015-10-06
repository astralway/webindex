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
LOCAL_DATA=$WI_HOME/data/
DATA_CONFIG=$WI_HOME/conf/data.yml
if [ ! -f $DATA_CONFIG ]; then
  echo "You must create $DATA_CONFIG"
  exit 1
fi

function get_prop {
  echo "`grep $1 $DATA_CONFIG | cut -d ' ' -f 2`"
}

function print_usage {
  echo -e "Usage: load.sh [--build]\n"
  echo "where:"
  echo "  --build   Optionally, force rebuild of jars"
  exit 1
}

command -v spark-submit >/dev/null 2>&1 || { echo >&2 "The 'spark-submit' command must be available on PATH.  Aborting."; exit 1; }

# Stop if any command after this fails
set -e

CC_SERVER_URL=`get_prop ccServerUrl`
CC_DATA_SET=`get_prop ccDataSet`
CC_TYPE=`get_prop ccDataType`
if [[ ! "wat wet warc" =~ $CC_TYPE ]]; then
  echo "Unknown file type: $CC_TYPE"
  print_usage
  exit 1
fi
CC_PATHS_FILE=$CC_TYPE.paths

export HADOOP_CONF_DIR=`get_prop hadoopConfDir`

BUILD=false
if [ ! -z $1 ]; then
  if [ "$1" == "--build" ]; then
    BUILD=true
  else
    echo "Unknown argument $1"
    exit 1
  fi
fi

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

if [ ! -d $LOCAL_DATA ]; then
  mkdir -p $LOCAL_DATA
fi

if [ ! -f $LOCAL_DATA/$CC_PATHS_FILE ]; then
  wget -c -P $LOCAL_DATA $CC_SERVER_URL/$CC_DATA_SET/$CC_PATHS_FILE.gz
  gzip -d $LOCAL_DATA/$CC_PATHS_FILE.gz
fi

echo "Using CC data set $CC_SERVER_URL/$CC_DATA_SET"

spark-submit --class io.fluo.webindex.data.Copy \
    --master yarn-client \
    --num-executors `get_prop sparkExecutorInstances` \
    --executor-memory `get_prop sparkExecutorMemory` \
    --conf spark.shuffle.service.enabled=true \
    $WI_DATA_DEP_JAR $WI_HOME/conf/data.yml file://$LOCAL_DATA/$CC_PATHS_FILE
