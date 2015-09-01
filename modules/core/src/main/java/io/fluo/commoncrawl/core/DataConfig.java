package io.fluo.commoncrawl.core;

import java.io.FileReader;

import com.esotericsoftware.yamlbeans.YamlReader;

public class DataConfig {

  public String fluoHome;
  public String fluoApp;
  public String accumuloIndexTable;
  public String watDataDir;
  public String wetDataDir;
  public String warcDataDir;
  public String hdfsTempDir;
  public String hadoopConfDir;


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
