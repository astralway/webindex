/*
 * Copyright 2015 Fluo authors (see AUTHORS)
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

package io.fluo.webindex.data.fluo;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;

import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.DataUtil;
import io.fluo.webindex.data.fluo.UriMap.UriInfo;
import io.fluo.webindex.data.recipes.Transmutable;
import io.fluo.webindex.data.spark.IndexUtil;
import io.fluo.webindex.data.util.LinkUtil;
import org.apache.accumulo.core.data.Mutation;
import org.slf4j.LoggerFactory;

public class UriCountExport implements Transmutable<String> {
  public UriInfo prevCount = UriInfo.EMPTY;
  public UriInfo newCount = UriInfo.EMPTY;

  public UriCountExport() {}

  public UriCountExport(UriInfo prevCount, UriInfo newCount) {
    this.prevCount = prevCount;
    this.newCount = newCount;
  }

  @Override
  public Collection<Mutation> toMutations(String uri, long seq) {
    ArrayList<Mutation> mutations = new ArrayList<>(4);

    createTotalUpdates(mutations, uri, seq, prevCount, newCount);
    Mutation m = createDomainUpdate(uri, seq, prevCount, newCount);
    if (m != null) {
      mutations.add(m);
    }
    mutations.add(createPageUpdate(uri, seq, newCount));

    return mutations;
  }

  // TODO maybe move code for mutating index table to central place.
  private static String getDomainRow(String pageUri) {
    String pageDomain;
    try {
      pageDomain = LinkUtil.getReverseTopPrivate(DataUtil.toUrl(pageUri));
      return "d:" + pageDomain;
    } catch (ParseException e) {
      LoggerFactory.getLogger(UriCountExport.class).warn(
          "Unable to get domain for " + pageUri + " " + e.getMessage());
    }

    return null;
  }

  private static Mutation createDomainUpdate(String uri, long seq, UriInfo prev, UriInfo curr) {
    String domain = getDomainRow(uri);
    if (domain == null) {
      return null;
    }
    Mutation m = new Mutation(getDomainRow(uri));
    // TODO screwy case when it does not exists... prev is 0 and initial val could be 0
    if (prev.linksTo != curr.linksTo) {
      String cf = String.format("%s:%s", IndexUtil.revEncodeLong(prev.linksTo), uri);
      m.putDelete(Constants.RANK, cf, seq);
    }

    String cf = String.format("%s:%s", IndexUtil.revEncodeLong(curr.linksTo), uri);
    if (curr.equals(UriInfo.EMPTY)) {
      m.putDelete(Constants.RANK, cf, seq);
    } else {
      m.put(Constants.RANK, cf, seq, "" + curr.linksTo);
    }
    return m;
  }

  private static Mutation createPageUpdate(String uri, long seq, UriInfo curr) {
    Mutation m = new Mutation("p:" + uri);
    if (curr.equals(UriInfo.EMPTY)) {
      m.putDelete(Constants.PAGE, Constants.INCOUNT, seq);
    } else {
      m.put(Constants.PAGE, Constants.INCOUNT, seq, "" + curr.linksTo);
    }

    return m;
  }

  private static String createTotalRow(String uri, long curr) {
    return String.format("t:%s:%s", IndexUtil.revEncodeLong(curr), uri);
  }

  private static void createTotalUpdates(ArrayList<Mutation> mutations, String uri, long seq,
      UriInfo prev, UriInfo curr) {
    Mutation m;

    // TODO screwy case when it does not exists... prev is 0 and initial val could be 0
    if (prev.linksTo != curr.linksTo) {
      m = new Mutation(createTotalRow(uri, prev.linksTo));
      m.putDelete("", "", seq);
      mutations.add(m);
    }

    m = new Mutation(createTotalRow(uri, curr.linksTo));
    if (curr.equals(UriInfo.EMPTY)) {
      m.putDelete("", "", seq);
    } else {
      m.put("", "", seq, "" + curr.linksTo);
    }
    mutations.add(m);
  }
}
