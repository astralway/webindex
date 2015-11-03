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

import java.io.File;
import java.util.Iterator;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.recipes.accumulo.export.TableInfo;
import io.fluo.webindex.core.DataConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintProps {

  private static final Logger log = LoggerFactory.getLogger(PrintProps.class);

  public static void main(String[] args) {

    if (args.length != 1) {
      log.error("Usage: Init <dataConfigPath>");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load(args[0]);
    FluoConfiguration fluoConfig = new FluoConfiguration(new File(dataConfig.getFluoPropsPath()));

    FluoConfiguration appConfig = new FluoConfiguration();

    FluoApp.configureApplication(appConfig,
        new TableInfo(fluoConfig.getAccumuloInstance(), fluoConfig.getAccumuloZookeepers(),
            fluoConfig.getAccumuloUser(), fluoConfig.getAccumuloPassword(),
            dataConfig.accumuloIndexTable), FluoApp.NUM_BUCKETS);

    Iterator<String> iter = appConfig.getKeys();

    while (iter.hasNext()) {
      String key = iter.next();
      System.out.println(key + " = " + appConfig.getProperty(key));
    }
  }
}
