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

: "${WI_HOME?"WI_HOME must be set"}"

. "$WI_HOME/bin/impl/base.sh"

if [[ "$OSTYPE" == "darwin"* ]]; then
  export SED="sed -i .bak"
else
  export SED="sed -i"
fi

# stop if any command fails
set -e

: "${SPARK_SUBMIT?"SPARK_SUBMIT must be set"}"
: "${WI_DATA_JAR?"WI_DATA_JAR must be set"}"
: "${WI_DATA_DEP_JAR?"WI_DATA_DEP_JAR must be set"}"

fluo_app=$(get_prop fluoApp)
fluo_cmd=$FLUO_HOME/bin/fluo
if [ ! -f "$fluo_cmd" ]; then
  echo "Fluo command script does not exist at $fluo_cmd"
  exit 1
fi

app_lib=$WI_HOME/target/lib
mkdir -p "$app_lib"
cp "$WI_DATA_JAR" "$app_lib"
mvn package -Pcopy-dependencies -DskipTests -DoutputDirectory="$app_lib"
# Add webindex core and its dependencies
cp "$WI_HOME/modules/core/target/webindex-core-$WI_VERSION.jar" "$app_lib"

app_props=$WI_HOME/target/fluo-app.properties
cp "$FLUO_HOME/conf/fluo-app.properties" "$app_props"
$SED "s#^.*fluo.observer.init.dir=[^ ]*#fluo.observer.init.dir=${app_lib}#" "$app_props"

java -cp "$app_lib/*:$("$fluo_cmd" classpath)" webindex.data.Configure "$WI_CONFIG" "$app_props"

"$fluo_cmd" init -a "$fluo_app" -p "$app_props" --force

"$SPARK_SUBMIT" --class webindex.data.Init $COMMON_SPARK_OPTS \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.executor.extraJavaOptions=-XX:+UseCompressedOops \
    $WI_DATA_DEP_JAR $1

echo "Webindex init has completed successfully."
