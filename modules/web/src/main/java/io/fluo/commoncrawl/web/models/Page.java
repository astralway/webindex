package io.fluo.commoncrawl.web.models;

public class Page {

  private String url;
  private Integer numInbound = 0;
  private Integer numOutbound = 0;
  private Integer score = 0;

  public Page(String url) {
    this.url = url;
  }

  public String getUrl() {
    return url;
  }

  public Integer getScore() {
    return score;
  }

  public void setScore(Integer score) {
    this.score = score;
  }

  public Integer getNumOutbound() {
    return numOutbound;
  }

  public void setNumOutbound(Integer numOutbound) {
    this.numOutbound = numOutbound;
  }

  public Integer getNumInbound() {
    return numInbound;
  }

  public void setNumInbound(Integer numInbound) {
    this.numInbound = numInbound;
  }
}
