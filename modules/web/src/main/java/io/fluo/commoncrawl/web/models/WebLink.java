package io.fluo.commoncrawl.web.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WebLink {

  private String url;
  private String anchorText;

  public WebLink(String url, String anchorText) {
    this.url = url;
    this.anchorText = anchorText;
  }

  @JsonProperty
  public String getUrl() {
    return url;
  }

  @JsonProperty
  public String getAnchorText() {
    return anchorText;
  }

}
