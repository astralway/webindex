package io.fluo.webindex.ui;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.fluo.webindex.core.DataConfig;
import org.hibernate.validator.constraints.NotEmpty;

public class WebIndexConfig extends Configuration {

  @NotEmpty
  private String dataConfigPath;

  @JsonProperty
  public String getDataConfigPath() {
    return dataConfigPath;
  }

  @JsonProperty
  public void getDataConfigPath(String dataConfigPath) {
    this.dataConfigPath = dataConfigPath;
  }

  public DataConfig getDataConfig() {
    return DataConfig.load(dataConfigPath);
  }
}
