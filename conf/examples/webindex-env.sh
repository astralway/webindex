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

# Set environment variables if they are not already set.  Please modify the
# export statement to use the correct directory.  Remove the test statement
# to override any previously set environment.

## Installation directories
test -z "$HADOOP_PREFIX" && export HADOOP_PREFIX=/path/to/hadoop
test -z "$HADOOP_CONF_DIR" && export HADOOP_CONF_DIR=/path/to/hadoop/etc/hadoop
test -z "$FLUO_HOME" && export FLUO_HOME=/path/to/fluo
test -z "$SPARK_HOME" && export SPARK_HOME=/path/to/spark

## Accumulo and Fluo versions that should be included in the shaded jar created for Spark.
export FLUO_VERSION=`$FLUO_HOME/bin/fluo version`
export ACCUMULO_VERSION=`accumulo version`

## Accumulo client will likely not work without correct thrift version
if [[ $ACCUMULO_VERSION < "1.8" ]]; then
  THRIFT_VERSION="0.9.1"
elif [[ $ACCUMULO_VERSION < "2.0" ]]; then
  THRIFT_VERSION="0.9.3"
else
  THRIFT_VERSION="0.10.0"
fi

## Spark
# Number of Spark executor instances
export WI_EXECUTOR_INSTANCES=2
# Amount of memory given to each Spark executor
export WI_EXECUTOR_MEMORY=512m
