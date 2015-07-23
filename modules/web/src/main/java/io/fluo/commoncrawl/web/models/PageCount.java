package io.fluo.commoncrawl.web.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PageCount {

  private String url;
  private Long count;

  public PageCount(String url, Long count) {
    this.url = url;
    this.count = count;
  }

  @JsonProperty
  public String getUrl() {
    return url;
  }

  @JsonProperty
  public Long getCount() {
    return count;
  }
}
