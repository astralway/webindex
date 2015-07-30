#!/bin/bash

BIN_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
CC_HOME=$( cd "$( dirname "$BIN_DIR" )" && pwd )

if [ ! -f $CC_HOME/conf/dropwizard.yml ]; then
  echo "Need to create dropwizard.yml in conf/" 
  exit 1
fi

cd $CC_HOME/modules/web
mvn clean package

java -jar $CC_HOME/modules/web/target/cc-web-0.0.1-SNAPSHOT.jar server $CC_HOME/conf/dropwizard.yml
