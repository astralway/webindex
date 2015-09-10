package io.fluo.webindex.core.models;

import java.util.ArrayList;
import java.util.List;

public class Pages {

  private String domain;
  private String next = "";
  private Integer pageNum;
  private Long total;
  private List<PageScore> pages = new ArrayList<>();

  public Pages() {
    // Jackson deserialization
  }

  public Pages(String domain, Integer pageNum) {
    this.domain = domain;
    this.pageNum = pageNum;
  }

  public Long getTotal() {
    return total;
  }

  public void setTotal(Long total) {
    this.total = total;
  }

  public String getDomain() {
    return domain;
  }

  public List<PageScore> getPages() {
    return pages;
  }

  public String getNext() {
    return next;
  }

  public void setNext(String next) {
    this.next = next;
  }

  public Integer getPageNum() {
    return pageNum;
  }

  public void addPage(PageScore pc) {
    pages.add(pc);
  }

  public void addPage(String url, Long score) {
    pages.add(new PageScore(url, score));
  }

  public class PageScore {

    private String url;
    private Long score;

    public PageScore(String url, Long score) {
      this.url = url;
      this.score = score;
    }

    public String getUrl() {
      return url;
    }

    public Long getScore() {
      return score;
    }
  }
}
