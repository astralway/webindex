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
: ${HADOOP_PREFIX?"HADOOP_PREFIX must be set"}

set -e

# defaults
LOAD_SRC=s3

while getopts ":d:i:l:s:e:m:fg" opt; do
  case $opt in
    d)
      PATHS_DATE=$OPTARG
      ;;
    i)  
      INIT_RANGE=$OPTARG
      ;;
    l)
      LOAD_RANGE=$OPTARG
      ;;
    s)
      LOAD_SRC=$OPTARG
      ;;
    f|g)
      ;;
    e)
      export WI_EXECUTOR_INSTANCES=$OPTARG
      ;;
    m)
      export WI_EXECUTOR_MEMORY=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG"
      exit 1
      ;;
  esac
done

if [ -z $PATHS_DATE ]; then  
  echo "-d <DATE> must be set"
  exit 1
fi
if [ -z $INIT_RANGE ]; then  
  echo "-i <RANGE> must be set"
  exit 1
fi
if [ -z $LOAD_RANGE ]; then  
  echo "-l <RANGE> must be set"
  exit 1
fi

WIC=$WI_HOME/bin/webindex
HDFS=$HADOOP_PREFIX/bin/hdfs

echo "Killing any running webindex Fluo applications or Spark jobs"
$WIC kill

echo "Retrieving paths file for $PATHS_DATE"
$WIC getpaths $PATHS_DATE

if [ "$INIT_RANGE" == "none" ]; then
  echo "Initializing webindex application with no data"
  $WIC init -fg
else
  INIT_PATH=/cc/data/$PATHS_DATE/$INIT_RANGE
  $HDFS dfs -test -d $INIT_PATH
  if [ $? != 0 ]; then
    echo "Copying data $PATHS_DATE $INIT_RANGE to $INIT_PATH"
    $WIC copy $PATHS_DATE $INIT_RANGE $INIT_PATH -fg
  fi
  echo "Initializing webindex application with HDFS data $INIT_PATH"
  $WIC init $INIT_PATH -fg
fi

echo "Starting UI.  Logs will go to seperate file"
$WIC ui

if [ "$LOAD_SRC" == "hdfs" -a "$LOAD_RANGE" != "none" ]; then
  LOAD_PATH=/cc/data/$PATHS_DATE/$LOAD_RANGE
  $HDFS dfs -test -d $LOAD_PATH
  if [ $? != 0 ]; then
    echo "Copying data $PATHS_DATE $LOAD_RANGE to $LOAD_PATH"
    $WIC copy $PATHS_DATE $LOAD_RANGE $LOAD_PATH -fg
  fi
  echo "Loading HDFS data $LOAD_PATH into Fluo"
  $WIC load-hdfs $LOAD_PATH -fg
elif [ "$LOAD_RANGE" != "none" ]; then
  echo "Loading S3 data $PATHS_DATE $LOAD_RANGE into Fluo"
  $WIC load-s3 $PATHS_DATE $LOAD_RANGE -fg
fi
