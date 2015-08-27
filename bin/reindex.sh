#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
CC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

function get_prop {
  DATA_CONFIG=$CC_HOME/conf/data.yml
  if [ ! -f $DATA_CONFIG ]; then
    echo "You must create $DATA_CONFIG"
    exit 1
  fi
  echo "`grep $1 $DATA_CONFIG | cut -d ' ' -f 2`"
}

export HADOOP_CONF_DIR=`get_prop hadoopConfDir`

cd $CC_HOME
mvn clean install
cd $CC_HOME/modules/data
mvn package assembly:single

CC_JAR=$CC_HOME/modules/data/target/cc-data-0.0.1-SNAPSHOT-jar-with-dependencies.jar
if [ ! -f $CC_JAR ]; then
  echo "Failed to build $CC_JAR"
  exit 1
fi
# If running OSX, remove file from Jar that causes problems
if [[ "$OSTYPE" == "darwin"* ]]; then
  zip -d $CC_JAR META-INF/LICENSE
fi

command -v spark-submit >/dev/null 2>&1 || { echo >&2 "The 'spark-submit' command must be available on PATH.  Aborting."; exit 1; }
spark-submit --class io.fluo.commoncrawl.data.Reindex \
    --master yarn-client \
    --num-executors 1 \
    --driver-memory 256m \
    --executor-memory 2g \
    --executor-cores 2 \
    --conf spark.executor.extraJavaOptions=-XX:+UseCompressedOops \
    $CC_JAR $CC_HOME/conf/data.yml
