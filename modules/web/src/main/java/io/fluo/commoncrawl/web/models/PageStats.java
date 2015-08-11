package io.fluo.commoncrawl.web.models;

import java.net.MalformedURLException;
import java.net.URL;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.net.InternetDomainName;

public class PageStats {

  private String url;
  private Integer numInbound = 0;
  private Integer numOutbound = 0;
  private Integer score = 0;

  public PageStats(String url) {
    this.url = url;
  }

  @JsonProperty
  public String getUrl() {
    return url;
  }

  @JsonProperty
  public Integer getScore() {
    return score;
  }

  public void setScore(Integer score) {
    this.score = score;
  }

  @JsonProperty
  public Integer getNumOutbound() {
    return numOutbound;
  }

  public void setNumOutbound(Integer numOutbound) {
    this.numOutbound = numOutbound;
  }

  @JsonProperty
  public Integer getNumInbound() {
    return numInbound;
  }

  public void setNumInbound(Integer numInbound) {
    this.numInbound = numInbound;
  }

  @JsonProperty
  public String getTopPrivate() {
    try {
      URL u = new URL(url);
      return InternetDomainName.from(u.getHost()).topPrivateDomain().toString();
    } catch (MalformedURLException e) {
      e.printStackTrace();
      return "unknown";
    }
  }
}
