package io.fluo.commoncrawl.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

public class InboundConfiguration extends Configuration {

  @NotEmpty
  private String fluoPropsPath;

  @JsonProperty
  public String getFluoPropsPath() {
    return fluoPropsPath;
  }

  @JsonProperty
  public void setFluoPropsPath(String fluoPropsPath) {
    this.fluoPropsPath = fluoPropsPath;
  }
}
