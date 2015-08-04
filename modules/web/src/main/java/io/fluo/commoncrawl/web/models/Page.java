package io.fluo.commoncrawl.web.models;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Page {

  private String url;

  private List<WebLink> links = new ArrayList<>();

  public Page() {
    // Jackson deserialization
  }

  public Page(String url) {
    this.url = url;
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
}
