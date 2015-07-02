#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
CC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )
if [ -f $CC_HOME/conf/env.sh ]; then
 . $CC_HOME/conf/env.sh
else
 . $CC_HOME/conf/env.sh.example
fi

CC_JAR=$CC_HOME/target/common-crawl-0.0.1-SNAPSHOT-jar-with-dependencies.jar
if [ ! -f $CC_JAR ]; then
  echo "Building jar"
  mvn package assembly:single
  if [ ! -f $CC_JAR ]; then
    echo "Failed to build $CC_JAR"
    exit 1
  fi 
fi

command -v yarn >/dev/null 2>&1 || { echo >&2 "The 'yarn' command must be available on PATH.  Aborting."; exit 1; }

yarn jar $CC_JAR io.fluo.commoncrawl.wordcount.WordCount $CC_DATA $CC_OUTPUT
