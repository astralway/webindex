#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
CC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )
if [ -f $CC_HOME/conf/env.sh ]; then
 . $CC_HOME/conf/env.sh
else
 . $CC_HOME/conf/env.sh.example
fi

function setup() {
  cd $CC_HOME/modules/data
  
  mvn clean package assembly:single

  CC_JAR=$CC_HOME/modules/data/target/cc-data-0.0.1-SNAPSHOT-jar-with-dependencies.jar
  if [ ! -f $CC_JAR ]; then
    echo "Failed to build $CC_JAR"
    exit 1
  fi
  # If running OSX, remove file from Jar that causes problems
  if [[ "$OSTYPE" == "darwin"* ]]; then
    zip -d $CC_JAR META-INF/LICENSE
  fi

  command -v hdfs >/dev/null 2>&1 || { echo >&2 "The 'hdfs' command must be available on PATH.  Aborting."; exit 1; }
  hdfs dfs -rm -r $CC_OUTPUT
}

case "$1" in
spark)
  setup
  command -v spark-submit >/dev/null 2>&1 || { echo >&2 "The 'spark-submit' command must be available on PATH.  Aborting."; exit 1; }
  spark-submit --class io.fluo.commoncrawl.data.spark.WordCount \
      --master yarn-cluster \
      --num-executors 1 \
      --driver-memory 500m \
      --executor-memory 2g \
      --executor-cores 1 \
      $CC_JAR \
      $CC_DATA/wet/ $CC_OUTPUT
  ;;
mapred)
  setup
  command -v yarn >/dev/null 2>&1 || { echo >&2 "The 'yarn' command must be available on PATH.  Aborting."; exit 1; }
  yarn jar $CC_JAR io.fluo.commoncrawl.data.mapred.WordCount $CC_DATA/wet/ $CC_OUTPUT
  ;;
*)
  echo -e "Usage: wordcount.sh <execution>\n"
  echo -e "where:\n"
  echo "  <execution>   How to execute (mapred or spark)"
  exit 1
esac
