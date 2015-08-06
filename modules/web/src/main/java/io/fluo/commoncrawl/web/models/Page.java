package io.fluo.commoncrawl.web.models;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Page {

  private String url;
  private String domain;
  private String next = "";
  private Integer pageNum;

  private List<WebLink> links = new ArrayList<>();

  public Page() {
    // Jackson deserialization
  }

  public Page(String url, String domain, Integer pageNum) {
    this.url = url;
    this.domain = domain;
    this.pageNum = pageNum;
  }

  @JsonProperty
  public String getUrl() {
    return url;
  }

  @JsonProperty
  public String getDomain() {
    return domain;
  }

  @JsonProperty
  public List<WebLink> getLinks() {
    return links;
  }

  public void addLink(WebLink link) {
    links.add(link);
  }

  @JsonProperty
  public Integer getPageNum() {
    return pageNum;
  }

  @JsonProperty
  public String getNext() {
    return next;
  }

  public void setNext(String next) {
    this.next = next;
  }
}
