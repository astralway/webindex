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

package webindex.core.models;

import java.io.Serializable;
import java.util.Optional;

import com.google.common.base.Preconditions;

/**
 * Used by URI collision free map
 */
public class UriInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final UriInfo ZERO = new UriInfo(0, 0);

  // the numbers of documents that link to this URI
  public long linksTo;

  // the number of documents with this URI. Should be 0 or 1
  public int docs;

  public UriInfo() {}

  public UriInfo(long linksTo, int docs) {
    this.linksTo = linksTo;
    this.docs = docs;
  }

  public void add(UriInfo other) {
    Preconditions.checkArgument(this != ZERO);
    this.linksTo += other.linksTo;
    this.docs += other.docs;
  }

  @Override
  public String toString() {
    return linksTo + " " + docs;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof UriInfo) {
      UriInfo oui = (UriInfo) o;
      return linksTo == oui.linksTo && docs == oui.docs;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return docs + (int) linksTo;
  }

  public static UriInfo merge(UriInfo u1, UriInfo u2) {
    UriInfo total = new UriInfo(0, 0);
    total.add(u1);
    total.add(u2);
    return total;
  }

  public static Optional<UriInfo> reduce(Iterable<UriInfo> uriInfos) {
    UriInfo sum = new UriInfo();
    for (UriInfo uriInfo : uriInfos) {
      sum.add(uriInfo);
    }
    return sum.equals(ZERO) ? Optional.empty() : Optional.of(sum);
  }
}
