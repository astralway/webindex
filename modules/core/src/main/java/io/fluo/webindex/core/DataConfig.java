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

import java.io.FileReader;

import com.esotericsoftware.yamlbeans.YamlReader;

public class DataConfig {

  public String fluoHome;
  public int numFilesToCopy;
  public String accumuloIndexTable;
  public String ccServerUrl;
  public String ccDataPaths;
  public String fluoApp;
  public String hdfsDataDir;
  public String hdfsTempDir;
  public int sparkExecutorInstances;
  public String sparkExecutorMemory;
  public boolean calculateAccumuloSplits;

  public String getFluoPropsPath() {
    return fluoHome + "/apps/" + fluoApp + "/conf/fluo.properties";
  }

  public static DataConfig load(String configPath) {
    try {
      YamlReader reader = new YamlReader(new FileReader(configPath));
      return reader.read(DataConfig.class);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
