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

package io.fluo.webindex.core;

import java.io.File;
import java.io.FileReader;

import com.esotericsoftware.yamlbeans.YamlReader;

public class DataConfig {

  public static String CC_URL_PREFIX = "https://aws-publicdatasets.s3.amazonaws.com/";

  public String fluoHome;
  public String hadoopConfDir;
  public String accumuloIndexTable;
  public String fluoApp;
  public String hdfsTempDir;
  public int sparkExecutorInstances;
  public String sparkExecutorMemory;

  public String getFluoPropsPath() {
    return addSlash(fluoHome) + "apps/" + fluoApp + "/conf/fluo.properties";
  }

  public static String getEnvPath(String name) {
    String path = System.getenv(name);
    if (path == null) {
      throw new IllegalStateException(name + " must be set in environment!");
    }
    if (!(new File(path).exists())) {
      throw new IllegalStateException("Directory set by " + name + "=" + path + " does not exist");
    }
    return path;
  }

  public static DataConfig load() {
    final String homePath = getEnvPath("WI_HOME");
    final String userPath = homePath + "/conf/data.yml";
    final String defaultPath = homePath + "/conf/data.yml.example";
    if ((new File(userPath).exists())) {
      return load(userPath);
    } else {
      return load(defaultPath);
    }
  }

  public static DataConfig load(String configPath) {
    return load(configPath, true);
  }

  protected static DataConfig load(String configPath, boolean useEnv) {
    try {
      YamlReader reader = new YamlReader(new FileReader(configPath));
      DataConfig config = reader.read(DataConfig.class);
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
