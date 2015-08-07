package io.fluo.commoncrawl.web.models;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.Length;

public class Pages {

  @Length(max = 100)
  private String domain;

  private String next = "";

  private Integer pageNum;

  private List<PageScore> pages = new ArrayList<>();

  public Pages() {
    // Jackson deserialization
  }

  public Pages(String domain, Integer pageNum) {
    this.domain = domain;
    this.pageNum = pageNum;
  }

  @JsonProperty
  public String getDomain() {
    return domain;
  }

  @JsonProperty
  public List<PageScore> getPages() {
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

  public void addPage(PageScore pc) {
    pages.add(pc);
  }
}
