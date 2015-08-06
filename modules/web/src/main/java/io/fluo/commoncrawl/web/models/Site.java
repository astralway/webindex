package io.fluo.commoncrawl.web.models;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.Length;

public class Site {

  @Length(max = 100)
  private String domain;

  private String next = "";

  private Integer pageNum;

  private List<PageCount> pages = new ArrayList<>();

  public Site() {
    // Jackson deserialization
  }

  public Site(String domain, Integer pageNum) {
    this.domain = domain;
    this.pageNum = pageNum;
  }

  @JsonProperty
  public String getDomain() {
    return domain;
  }

  @JsonProperty
  public List<PageCount> getPages() {
    return pages;
  }

  @JsonProperty
  public String getNext() {
    return next;
  }

  public void setNext(String next) {
    this.next = next;
  }

  @JsonProperty
  public Integer getPageNum() {
    return pageNum;
  }

  public void addPage(PageCount pc) {
    pages.add(pc);
  }
}
