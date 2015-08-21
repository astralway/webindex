package io.fluo.commoncrawl.core.models;

import java.util.ArrayList;
import java.util.List;

public class TopResults {

  private String resultType;
  private String next = "";
  private Integer pageNum;
  private List<Result> results = new ArrayList<>();

  public String getResultType() {
    return resultType;
  }

  public void setResultType(String resultType) {
    this.resultType = resultType;
  }

  public Integer getPageNum() {
    return pageNum;
  }

  public void setPageNum(Integer pageNum) {
    this.pageNum = pageNum;
  }

  public String getNext() {
    return next;
  }

  public void addResult(String key, Long value) {
    results.add(new Result(key, value));
  }

  public List<Result> getResults() {
    return results;
  }

  public void setNext(String next) {
    this.next = next;
  }

  public class Result {

    private String key;
    private Long value;

    Result(String key, Long value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public Long getValue() {
      return value;
    }
  }
}
