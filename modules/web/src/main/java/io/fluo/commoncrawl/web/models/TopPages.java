package io.fluo.commoncrawl.web.models;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.Length;

public class TopPages {

  @Length(max = 100)
  private String domain;

  private List<PageCount> pages = new ArrayList<>();

  public TopPages() {
    // Jackson deserialization
  }

  public TopPages(String domain) {
    this.domain = domain;
  }

  @JsonProperty
  public String getDomain() {
    return domain;
  }

  @JsonProperty
  public List<PageCount> getPages() {
    return pages;
  }

  public void addPage(PageCount pc) {
    pages.add(pc);
  }
}
