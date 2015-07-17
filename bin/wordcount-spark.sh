#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
CC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )
if [ -f $CC_HOME/conf/env.sh ]; then
 . $CC_HOME/conf/env.sh
else
 . $CC_HOME/conf/env.sh.example
fi

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

command -v spark-submit >/dev/null 2>&1 || { echo >&2 "The 'spark-submit' command must be available on PATH.  Aborting."; exit 1; }
command -v spark-submit >/dev/null 2>&1 || { echo >&2 "The 'hdfs' command must be available on PATH.  Aborting."; exit 1; }

hdfs dfs -rm -r $CC_OUTPUT

spark-submit --class io.fluo.commoncrawl.spark.WordCount \
    --master yarn-cluster \
    --num-executors 1 \
    --driver-memory 500m \
    --executor-memory 2g \
    --executor-cores 1 \
    $CC_JAR \
    $CC_DATA/wet/ $CC_OUTPUT
