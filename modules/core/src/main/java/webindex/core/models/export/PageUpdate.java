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

import java.util.List;

import webindex.core.models.Link;

/**
 * Represents index updates for pages
 */
public class PageUpdate implements IndexUpdate {

  private String uri;
  private String json;
  private List<Link> addedLinks;
  private List<Link> deletedLinks;

  public PageUpdate() {} // For serialization

  public PageUpdate(String uri, String json, List<Link> addedLinks, List<Link> deletedLinks) {
    this.uri = uri;
    this.json = json;
    this.addedLinks = addedLinks;
    this.deletedLinks = deletedLinks;
  }

  public String getUri() {
    return uri;
  }

  public String getJson() {
    return json;
  }

  public List<Link> getAddedLinks() {
    return addedLinks;
  }

  public List<Link> getDeletedLinks() {
    return deletedLinks;
  }
}
