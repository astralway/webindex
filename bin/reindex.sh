#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
WI_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

function get_prop {
  DATA_CONFIG=$WI_HOME/conf/data.yml
  if [ ! -f $DATA_CONFIG ]; then
    echo "You must create $DATA_CONFIG"
    exit 1
  fi
  echo "`grep $1 $DATA_CONFIG | cut -d ' ' -f 2`"
}

export HADOOP_CONF_DIR=`get_prop hadoopConfDir`

cd $WI_HOME
mvn clean install
cd $WI_HOME/modules/data
mvn package assembly:single

WI_JAR=$WI_HOME/modules/data/target/webindex-data-0.0.1-SNAPSHOT-jar-with-dependencies.jar
if [ ! -f $WI_JAR ]; then
  echo "Failed to build $WI_JAR"
  exit 1
fi
# If running OSX, remove file from Jar that causes problems
if [[ "$OSTYPE" == "darwin"* ]]; then
  zip -d $WI_JAR META-INF/LICENSE
fi

command -v spark-submit >/dev/null 2>&1 || { echo >&2 "The 'spark-submit' command must be available on PATH.  Aborting."; exit 1; }
spark-submit --class io.fluo.webindex.data.Reindex \
    --master yarn-client \
    --num-executors 1 \
    --driver-memory 256m \
    --executor-memory 2g \
    --executor-cores 2 \
    --conf spark.executor.extraJavaOptions=-XX:+UseCompressedOops \
    $WI_JAR $WI_HOME/conf/data.yml
