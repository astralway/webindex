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

package webindex.core;

import java.io.File;
import java.io.FileReader;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebIndexConfig {

  private static final Logger log = LoggerFactory.getLogger(WebIndexConfig.class);

  public static String CC_URL_PREFIX = "https://commoncrawl.s3.amazonaws.com/";
  public static final String WI_EXECUTOR_INSTANCES = "WI_EXECUTOR_INSTANCES";

  public String fluoHome;
  public String hadoopConfDir;
  public String accumuloIndexTable;
  public String fluoApp;
  public int numTablets = -1;
  public int numBuckets = -1;
  public String hdfsTempDir;
  public int loadRateLimit = 0;

  public String getConnPropsPath() {
    return addSlash(fluoHome) + "conf/fluo-conn.properties";
  }

  public int getNumExecutorInstances() {
    String numInstances = getEnv(WI_EXECUTOR_INSTANCES);
    try {
      return Integer.parseInt(numInstances);
    } catch (NumberFormatException e) {
      throw new IllegalStateException("Failed to parse value of " + numInstances + " for "
          + WI_EXECUTOR_INSTANCES);
    }
  }

  public int getLoadRateLimit() {
    return loadRateLimit;
  }

  public static String getEnv(String name) {
    String value = System.getenv(name);
    if (value == null) {
      throw new IllegalStateException(name + " must be set in environment!");
    }
    return value;
  }

  public static String getEnvPath(String name) {
    String path = getEnv(name);
    if (!(new File(path).exists())) {
      throw new IllegalStateException("Directory set by " + name + "=" + path + " does not exist");
    }
    return path;
  }

  public static WebIndexConfig load() {
    final String homePath = getEnvPath("WI_HOME");
    final String userPath = homePath + "/conf/webindex.yml";
    final String defaultPath = homePath + "/conf/examples/webindex.yml";
    if ((new File(userPath).exists())) {
      log.info("Using user config at {}", userPath);
      return load(userPath);
    } else {
      log.info("Using default config at {}", defaultPath);
      return load(defaultPath);
    }
  }

  public static WebIndexConfig load(String configPath) {
    return load(configPath, true);
  }

  protected static WebIndexConfig load(String configPath, boolean useEnv) {
    Preconditions.checkArgument(new File(configPath).exists(), "Config does not exist at "
        + configPath);
    try {
      YamlReader reader = new YamlReader(new FileReader(configPath));
      WebIndexConfig config = reader.read(WebIndexConfig.class);
      if (useEnv) {
        config.hadoopConfDir = getEnvPath("HADOOP_CONF_DIR");
        config.fluoHome = getEnvPath("FLUO_HOME");
      }
      return config;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static String addSlash(String prefix) {
    if (!prefix.endsWith("/")) {
      return prefix + "/";
    }
    return prefix;
  }
}
