#!/bin/bash


function print_usage {
  echo -e "Usage: load.sh <fileType> <numFiles>\n"
  echo "where:"
  echo "  <fileType>    Common crawl file type (ie. warc, wet, wat)"
  echo "  <numFiles>    Number of files to load"
  exit 1
}

CC_URL_PREFIX=https://aws-publicdatasets.s3.amazonaws.com
CC_PATHS_PREFIX=common-crawl/crawl-data/CC-MAIN-2015-18

if [ -z "$1" ]; then
  echo -e "Argument <fileType> cannot be empty\n"
  print_usage
fi

CC_TYPE=$1
if [[ ! "wat wet warc" =~ $1 ]]; then
  echo "Unknown file type: $CC_TYPE"
  print_usage
  exit 1
fi
CC_PATHS_FILE=$CC_TYPE.paths

if [ -z "$2" ]; then
  echo -e "Argument <numFiles> cannot be empty\n"
  print_usage
fi
NUM_FILES=$2

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
CC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )
LOCAL_DATA=$CC_HOME/data/

if [ -f $CC_HOME/conf/env.sh ]; then
 . $CC_HOME/conf/env.sh
else
 . $CC_HOME/conf/env.sh.example
fi

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
