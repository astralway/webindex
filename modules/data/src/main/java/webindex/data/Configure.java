/*
 * Copyright 2015 Webindex authors (see AUTHORS)
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

package webindex.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.config.FluoConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import webindex.core.WebIndexConfig;
import webindex.data.spark.IndexEnv;

public class Configure {

  private static final Logger log = LoggerFactory.getLogger(Configure.class);

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      log.error("Usage: Configure <webindexConfigPath> <fluoAppProps>");
      System.exit(1);
    }
    WebIndexConfig webIndexConfig = WebIndexConfig.load(args[0]);
    String appPropsPath = args[1];
    Preconditions.checkArgument(new File(appPropsPath).exists(), "File does not exist: "
        + appPropsPath);

    FluoConfiguration fluoConfig =
        new FluoConfiguration(new File(webIndexConfig.getConnPropsPath()));
    fluoConfig.load(new File(appPropsPath));


    IndexEnv env = new IndexEnv(webIndexConfig, fluoConfig);
    env.initAccumuloIndexTable();

    FluoConfiguration appConfig = new FluoConfiguration();
    env.configureApplication(fluoConfig, appConfig);
    Iterator<String> iter = appConfig.getKeys();
    try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(appPropsPath, true)))) {
      while (iter.hasNext()) {
        String key = iter.next();
        out.println(key + " = " + appConfig.getRawString(key));
      }
    }
  }
}
