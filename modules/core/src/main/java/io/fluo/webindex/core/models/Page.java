/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.webindex.core.models;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.Gson;
import io.fluo.webindex.core.DataUtil;

public class Page implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final Page EMPTY = new Page();
  public static final Page DELETE = new Page(true);
  public static final String DELETE_JSON = "delete";

  private String url = "";
  private String domain;
  private Long numInbound;
  private Long numOutbound = new Long(0);
  private String crawlDate;
  private String server;
  private String title;
  private Set<Link> outboundLinks = new HashSet<>();
  private transient boolean isDelete = false;

  private Page() {}

  private Page(boolean isDelete) {
    this.isDelete = isDelete;
  }

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

  /**
   * @return True if page did not already contain link
   */
  public boolean addOutboundLink(String url, String anchorText) {
    boolean added = outboundLinks.add(new Link(url, anchorText));
    if (added) {
      numOutbound++;
    }
    return added;
  }

  /**
   * @return True if link was removed
   */
  public boolean removeOutboundLink(String url) {
    boolean removed = outboundLinks.remove(new Link(url));
    if (removed) {
      numOutbound--;
    }
    return removed;
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

  public boolean isDelete() {
    return isDelete;
  }

  public static Page fromJson(Gson gson, String pageJson) {
    if (pageJson.isEmpty()) {
      return Page.EMPTY;
    }

    if (pageJson.equals(DELETE_JSON)) {
      return Page.DELETE;
    }

    return gson.fromJson(pageJson, Page.class);
  }

  public static class Link implements Serializable {

    private static final long serialVersionUID = 1L;

    private String url;
    private String anchorText;

    public Link(String url, String anchorText) {
      this.url = DataUtil.cleanUrl(url);
      this.anchorText = anchorText;
    }

    public Link(String url) {
      this(url, "");
    }

    public Link() {}

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
