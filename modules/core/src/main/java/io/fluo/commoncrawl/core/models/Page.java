package io.fluo.commoncrawl.core.models;

import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Set;

import io.fluo.commoncrawl.core.DataUtil;

public class Page {

  public static Page EMPTY = new Page();
  private String url = "";
  private String domain;
  private Long numInbound;
  private Long numOutbound = new Long(0);
  private Long score;
  private String crawlDate;
  private String server;
  private String title;
  private Set<Link> outboundLinks = new HashSet<>();

  private Page() {}

  public Page(String url) {
    this.url = url;
  }

  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public String getUrl() {
    return url;
  }

  public String getUri() throws MalformedURLException {
    return DataUtil.toUri(url);
  }

  public Set<Link> getOutboundLinks() {
    return outboundLinks;
  }

  public void addOutboundLink(String url, String anchorText) {
    if (outboundLinks.add(new Link(url, anchorText))) {
      numOutbound++;
    }
  }

  public boolean isEmpty() {
    return url.isEmpty() && outboundLinks.isEmpty();
  }

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public Long getNumInbound() {
    return numInbound;
  }

  public void setNumInbound(Long numInbound) {
    this.numInbound = numInbound;
  }

  public Long getNumOutbound() {
    return numOutbound;
  }

  public Long getScore() {
    return score;
  }

  public void setScore(Long score) {
    this.score = score;
  }

  public String getCrawlDate() {
    return crawlDate;
  }

  public void setCrawlDate(String crawlDate) {
    this.crawlDate = crawlDate;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public class Link {

    private String url;
    private String anchorText;

    public Link(String url, String anchorText) {
      this.url = DataUtil.cleanUrl(url);
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
