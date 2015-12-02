/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.webindex.data;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.webindex.core.DataConfig;
import io.fluo.webindex.data.spark.IndexEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configure {

  private static final Logger log = LoggerFactory.getLogger(Configure.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      log.error("Usage: Configure");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load();

    IndexEnv env = new IndexEnv(dataConfig);
    env.initAccumuloIndexTable();

    FluoConfiguration appConfig = new FluoConfiguration();
    env.configureApplication(appConfig);

    Iterator<String> iter = appConfig.getKeys();
    try (PrintWriter out =
        new PrintWriter(new BufferedWriter(new FileWriter(dataConfig.getFluoPropsPath(), true)))) {
      while (iter.hasNext()) {
        String key = iter.next();
        out.println(key + " = " + appConfig.getProperty(key));
      }
    }
  }
}
