#!/bin/bash

CC_URL_PREFIX=https://aws-publicdatasets.s3.amazonaws.com
CC_PATHS_PREFIX=common-crawl/crawl-data/CC-MAIN-2015-18
CC_PATHS_FILE=wet.paths

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
CC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )


if [ -f $CC_HOME/conf/env.sh ]; then
 . $CC_HOME/conf/env.sh
else
 . $CC_HOME/conf/env.sh.example
fi

LOCAL_DATA=$CC_HOME/data/

if [ -z "$1" ]; then
  echo "Usage: load.sh <numFiles>"
  exit 1
fi

NUM_FILES=$1

command -v hdfs >/dev/null 2>&1 || { echo >&2 "The 'hdfs' command must be available on PATH.  Aborting."; exit 1; }

if [ ! -d $LOCAL_DATA ]; then
  mkdir -p $LOCAL_DATA
fi

hdfs dfs -ls $CC_DATA > /dev/null
if [ $? -ne 0 ]; then
  hdfs dfs -mkdir -p $CC_DATA
fi

if [ ! -f $LOCAL_DATA/$CC_PATHS_FILE ]; then
  wget -c -P $LOCAL_DATA $CC_URL_PREFIX/$CC_PATHS_PREFIX/$CC_PATHS_FILE.gz
  gzip -d $LOCAL_DATA/$CC_PATHS_FILE.gz
fi

head -n $NUM_FILES $LOCAL_DATA/$CC_PATHS_FILE | while read line
do
  CC_URL=$line
  CC_FILE=`echo $line | cut -d / -f 7`
  hdfs dfs -ls $CC_DATA/$CC_FILE > /dev/null 
  if [ $? -ne 0 ]; then
    echo "Downloading and loading: $CC_FILE"
    wget -c -P $LOCAL_DATA $CC_URL_PREFIX/$CC_URL
    hdfs dfs -put $LOCAL_DATA/$CC_FILE $CC_DATA/$CC_FILE
  else
    echo "File exists in HDFS: $CC_FILE"
  fi
done
