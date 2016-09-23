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

import webindex.core.models.UriInfo;

/**
 * Represents index updates for URIs
 */
public class UriUpdate implements IndexUpdate {

  private String uri;
  private UriInfo oldInfo;
  private UriInfo newInfo;

  public UriUpdate() {} // For serialization

  public UriUpdate(String uri, UriInfo oldInfo, UriInfo newInfo) {
    this.uri = uri;
    this.oldInfo = oldInfo;
    this.newInfo = newInfo;
  }

  public String getUri() {
    return uri;
  }

  public UriInfo getOldInfo() {
    return oldInfo;
  }

  public UriInfo getNewInfo() {
    return newInfo;
  }
}
