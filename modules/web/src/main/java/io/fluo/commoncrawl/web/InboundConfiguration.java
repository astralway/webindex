package io.fluo.commoncrawl.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.fluo.commoncrawl.core.DataConfig;
import org.hibernate.validator.constraints.NotEmpty;

public class InboundConfiguration extends Configuration {

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
