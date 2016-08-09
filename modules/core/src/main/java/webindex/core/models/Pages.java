/*
 * Copyright 2015 Webindex authors (see AUTHORS)
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

package webindex.core.models;

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
