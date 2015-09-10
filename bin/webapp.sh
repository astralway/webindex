#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
WI_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

if [ ! -f $WI_HOME/conf/dropwizard.yml ]; then
  echo "Need to create dropwizard.yml in conf/" 
  exit 1
fi

cd $WI_HOME/modules/ui
mvn clean package

java -jar $WI_HOME/modules/ui/target/webindex-ui-0.0.1-SNAPSHOT.jar server $WI_HOME/conf/dropwizard.yml
