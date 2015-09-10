package io.fluo.webindex.core.models;

import java.util.ArrayList;
import java.util.List;

public class Links {

  private String url;
  private String linkType;
  private String next = "";
  private Integer pageNum;
  private Long total;
  private List<WebLink> links = new ArrayList<>();

  public Links() {
    // Jackson deserialization
  }

  public Links(String url, String linkType, Integer pageNum) {
    this.url = url;
    this.linkType = linkType;
    this.pageNum = pageNum;
  }

  public Long getTotal() {
    return total;
  }

  public void setTotal(Long total) {
    this.total = total;
  }

  public String getUrl() {
    return url;
  }

  public List<WebLink> getLinks() {
    return links;
  }

  public void addLink(WebLink link) {
    links.add(link);
  }

  public void addLink(String url, String anchorText) {
    links.add(new WebLink(url, anchorText));
  }

  public String getLinkType() {
    return linkType;
  }

  public Integer getPageNum() {
    return pageNum;
  }

  public String getNext() {
    return next;
  }

  public void setNext(String next) {
    this.next = next;
  }

  public class WebLink {

    private String url;
    private String anchorText;

    public WebLink(String url, String anchorText) {
      this.url = url;
      this.anchorText = anchorText;
    }

    public String getUrl() {
      return url;
    }

    public String getAnchorText() {
      return anchorText;
    }
  }
}
