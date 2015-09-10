#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
WI_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

DATA_CONFIG=$WI_HOME/conf/data.yml
if [ ! -f $DATA_CONFIG ]; then
  echo "You must create $DATA_CONFIG"
  exit 1
fi

function get_prop {
  echo "`grep $1 $DATA_CONFIG | cut -d ' ' -f 2`"
}

hash spark-submit 2>/dev/null || { echo >&2 "Spark must be installed & spark-submit command must be on path.  Aborting."; exit 1; }
hash mvn 2>/dev/null || { echo >&2 "Maven must be installed & mvn command must be on path.  Aborting."; exit 1; }

export HADOOP_CONF_DIR=`get_prop hadoopConfDir`

# stop if any command fails
set -e

cd $WI_HOME
mvn clean install -DskipTests
cd $WI_HOME/modules/data
mvn package assembly:single -DskipTests
WI_DATA_JAR=$WI_HOME/modules/data/target/webindex-data-0.0.1-SNAPSHOT.jar
if [ ! -f $WI_DATA_JAR ]; then
  echo "Failed to build $WI_DATA_JAR"
  exit 1
fi
WI_DATA_DEP_JAR=$WI_HOME/modules/data/target/webindex-data-0.0.1-SNAPSHOT-jar-with-dependencies.jar
if [ ! -f $WI_DATA_DEP_JAR ]; then
  echo "Failed to build $WI_DATA_DEP_JAR"
  exit 1
fi
# If running OSX, remove file from Jar that causes problems
if [[ "$OSTYPE" == "darwin"* ]]; then
  zip -d $WI_DATA_DEP_JAR META-INF/LICENSE
fi

FLUO_HOME=`get_prop fluoHome`
if [ ! -d $FLUO_HOME ]; then
  echo "Fluo home does not exist at $FLUO_HOME"
  exit 1
fi

FLUO_APP=`get_prop fluoApp`
if [ -z $FLUO_APP ]; then
  echo "The property fluoApp must be set data.yml"
  exit 1
fi
FLUO_CMD=$FLUO_HOME/bin/fluo

$FLUO_CMD stop $FLUO_APP || true

FLUO_APP_HOME=$FLUO_HOME/apps/$FLUO_APP
if [ -d $FLUO_APP_HOME ]; then
  echo "Deleting existing $FLUO_APP_HOME"
  rm -rf $FLUO_APP_HOME
fi

$FLUO_CMD new $FLUO_APP

FLUO_APP_LIB=$FLUO_APP_HOME/lib
cp $WI_DATA_JAR $FLUO_APP_LIB
mvn dependency:get -Dartifact=io.fluo:fluo-recipes-core:1.0.0-beta-1-SNAPSHOT:jar -Ddest=$FLUO_APP_LIB
mvn dependency:get -Dartifact=io.fluo:fluo-recipes-accumulo:1.0.0-beta-1-SNAPSHOT:jar -Ddest=$FLUO_APP_LIB

APP_PROPS=$FLUO_APP_HOME/conf/fluo.properties
echo "io.fluo.observer.0=io.fluo.webindex.data.fluo.PageObserver" >> $APP_PROPS
echo "io.fluo.observer.1=io.fluo.webindex.data.fluo.InlinksObserver" >> $APP_PROPS
echo "io.fluo.observer.2=io.fluo.webindex.data.fluo.IndexExporter" >> $APP_PROPS
java -cp $WI_DATA_DEP_JAR io.fluo.webindex.data.PrintProps $WI_HOME/conf/data.yml >> $APP_PROPS

$FLUO_CMD init $FLUO_APP --force

command -v spark-submit >/dev/null 2>&1 || { echo >&2 "The 'spark-submit' command must be available on PATH.  Aborting."; exit 1; }
spark-submit --class io.fluo.webindex.data.Init \
    --master yarn-client \
    --num-executors 1 \
    --driver-memory 256m \
    --executor-memory 2g \
    --executor-cores 2 \
    --conf spark.executor.extraJavaOptions=-XX:+UseCompressedOops \
    $WI_DATA_DEP_JAR $WI_HOME/conf/data.yml

$FLUO_CMD start $FLUO_APP
