package io.fluo.commoncrawl.core;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Page {

  public static Page EMPTY = new Page();
  private String pageUrl = "";
  private Set<Link> externalLinks = new HashSet<>();

  private Page() {
  }

  public Page(String pageUrl) {
    this.pageUrl = pageUrl;
  }

  public String getPageUrl() {
    return pageUrl;
  }

  public String getPageUri() throws MalformedURLException {
    return DataUtil.toUri(pageUrl);
  }

  public Set<Link> getExternalLinks() {
    return externalLinks;
  }

  public void addExternalLink(String url, String anchorText) {
    externalLinks.add(new Link(url, anchorText));
  }

  public boolean isEmpty() {
    return pageUrl.isEmpty() && externalLinks.isEmpty();
  }

  public class Link {

    private String url;
    private String anchorText;

    public Link(String url, String anchorText) {
      this.url = url;
      this.anchorText = anchorText;
    }

    public String getUrl() {
      return url;
    }

    public String getUri() throws MalformedURLException {
      return DataUtil.toUri(url);
    }

    public String getAnchorText() {
      return anchorText;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Link) {
        Link other = (Link) o;
        return url.equals(other.url);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return url.hashCode();
    }
  }
}
