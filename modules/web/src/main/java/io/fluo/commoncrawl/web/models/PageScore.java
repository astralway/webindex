package io.fluo.commoncrawl.web.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PageScore {

  private String url;
  private Long score;

  public PageScore(String url, Long score) {
    this.url = url;
    this.score = score;
  }

  @JsonProperty
  public String getUrl() {
    return url;
  }

  @JsonProperty
  public Long getScore() {
    return score;
  }
}
