package io.fluo.webindex.core.models;

public class DomainStats {

  private String domain;
  private Long total = (long) 0;

  public DomainStats(String domain) {
    this.domain = domain;
  }

  public Long getTotal() {
    return total;
  }

  public void setTotal(Long total) {
    this.total = total;
  }
}
