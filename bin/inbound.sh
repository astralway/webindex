#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
CC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )
if [ -f $CC_HOME/conf/env.sh ]; then
 . $CC_HOME/conf/env.sh
else
 . $CC_HOME/conf/env.sh.example
fi

function setup() {
  cd $CC_HOME
  mvn clean

  CC_JAR=$CC_HOME/target/common-crawl-0.0.1-SNAPSHOT-jar-with-dependencies.jar
  if [ ! -f $CC_JAR ]; then
    echo "Building jar"
    mvn package assembly:single
    if [ ! -f $CC_JAR ]; then
      echo "Failed to build $CC_JAR"
      exit 1
    fi
    # If running OSX, remove file from Jar that causes problems
    if [[ "$OSTYPE" == "darwin"* ]]; then
      zip -d $CC_JAR META-INF/LICENSE
    fi
  fi

  command -v hdfs >/dev/null 2>&1 || { echo >&2 "The 'hdfs' command must be available on PATH.  Aborting."; exit 1; }
  hdfs dfs -rm -r -skipTrash $CC_OUTPUT
  echo "Deleted $CC_OUTPUT"
  hdfs dfs -mkdir -p /cc/failures
}

function usage() {
  echo -e "Usage: inbound.sh <execution> <outputDest>\n"
  echo -e "where:\n"
  echo "  <execution>   Execution engine to use (i.e. mapred or spark)"
  echo "  <outputDest>  Output destination of data (i.e hdfs, fluo)"
  exit 1
}

case "$2" in
fluo)
;;
hdfs)
;;
*)
  echo "Unknown outputDest - $2"
  usage
esac

case "$1" in
spark)
  setup
  command -v spark-submit >/dev/null 2>&1 || { echo >&2 "The 'spark-submit' command must be available on PATH.  Aborting."; exit 1; }
  spark-submit --class io.fluo.commoncrawl.spark.InboundLinks \
      --master yarn-cluster \
      --num-executors 1 \
      --driver-memory 500m \
      --executor-memory 1g \
      --executor-cores 2 \
      $CC_JAR \
      $CC_DATA/wat/ $2 $CC_OUTPUT $CC_FLUO_PROPS
  ;;
mapred)
  setup
  command -v yarn >/dev/null 2>&1 || { echo >&2 "The 'yarn' command must be available on PATH.  Aborting."; exit 1; }
  yarn jar $CC_JAR io.fluo.commoncrawl.mapred.InboundLinks $CC_DATA/wat/ $CC_OUTPUT
  ;;
*)
  echo "Unknown execution - $1"
  usage
esac
