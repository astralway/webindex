/*
 * Copyright 2016 Webindex authors (see AUTHORS)
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

package webindex.core.models.export;

/**
 * Represents index updates for domain
 */
public class DomainUpdate implements IndexUpdate {

  private String domain;
  private Long oldPageCount;
  private Long newPageCount;

  public DomainUpdate() {} // For serialization

  public DomainUpdate(String domain, Long oldPageCount, Long newPageCount) {
    this.domain = domain;
    this.oldPageCount = oldPageCount;
    this.newPageCount = newPageCount;
  }

  public String getDomain() {
    return domain;
  }

  public Long getOldPageCount() {
    return oldPageCount;
  }

  public Long getNewPageCount() {
    return newPageCount;
  }
}
