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

: ${WI_HOME?"WI_HOME must be set"}

set -e

while getopts ":d:i:l:e:m:fg" opt; do
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
    f|g)
      ;;
    e)
      export SPARK_EXECUTOR_INSTANCES=$OPTARG
      ;;
    m)
      export SPARK_EXECUTOR_MEMORY=$OPTARG
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

echo "Killing any running webindex Fluo applications or Spark jobs"
$WIC kill

echo "Retrieving paths file for $PATHS_DATE"
$WIC getpaths $PATHS_DATE

if [ "$INIT_RANGE" == "none" ]; then
  echo "Initializing webindex application with no data"
  $WIC init -fg
else
  DATA_PATH=/cc/data/$PATHS_DATE/$INIT_RANGE
  echo "Copying data $PATHS_DATE $INIT_RANGE to $DATA_PATH"
  $WIC copy $PATHS_DATE $INIT_RANGE $DATA_PATH -fg
  echo "Initializing webindex application with HDFS data $DATA_PATH"
  $WIC init $DATA_PATH -fg
fi

echo "Starting UI.  Logs will go to seperate file"
$WIC ui

if [ "$LOAD_RANGE" != "none" ]; then
  echo "Loading S3 data $PATHS_DATE $LOAD_RANGE into Fluo"
  $WIC load-s3 $PATHS_DATE $LOAD_RANGE -fg
fi
