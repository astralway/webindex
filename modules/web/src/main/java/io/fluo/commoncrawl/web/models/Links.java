package io.fluo.commoncrawl.web.models;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Links {

  private String url;
  private String linkType;
  private String next = "";
  private Integer pageNum;
  private Integer total;
  private List<WebLink> links = new ArrayList<>();

  public Links() {
    // Jackson deserialization
  }

  public Links(String url, String linkType, Integer pageNum) {
    this.url = url;
    this.linkType = linkType;
    this.pageNum = pageNum;
  }

  @JsonProperty
  public Integer getTotal() {
    return total;
  }

  public void setTotal(Integer total) {
    this.total = total;
  }

  @JsonProperty
  public String getUrl() {
    return url;
  }

  @JsonProperty
  public List<WebLink> getLinks() {
    return links;
  }

  public void addLink(WebLink link) {
    links.add(link);
  }

  @JsonProperty
  public String getLinkType() {
    return linkType;
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
