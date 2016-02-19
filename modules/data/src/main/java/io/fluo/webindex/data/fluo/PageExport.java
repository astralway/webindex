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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Sets.SetView;
import io.fluo.recipes.accumulo.export.AccumuloExport;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.models.Link;
import io.fluo.webindex.core.models.Page;
import org.apache.accumulo.core.data.Mutation;

public class PageExport implements AccumuloExport<String> {

  private String json;
  private List<Link> addedLinks;
  private List<Link> deletedLinks;

  public PageExport() {}

  public PageExport(String json, SetView<Link> addedLinks, SetView<Link> deletedLinks) {
    this.json = json;
    this.addedLinks = new ArrayList<>(addedLinks);
    this.deletedLinks = new ArrayList<>(deletedLinks);
  }

  @Override
  public Collection<Mutation> toMutations(String referencingUri, long seq) {

    ArrayList<Mutation> mutations = new ArrayList<>(addedLinks.size() + deletedLinks.size() + 1);

    Mutation jsonMutation = new Mutation("p:" + referencingUri);
    if (json.equals(Page.DELETE_JSON)) {
      jsonMutation.putDelete(Constants.PAGE, Constants.CUR, seq);
    } else {
      jsonMutation.put(Constants.PAGE, Constants.CUR, seq, json);
    }
    mutations.add(jsonMutation);

    // invert links on export
    for (Link link : addedLinks) {
      Mutation m = new Mutation("p:" + link.getPageID());
      m.put(Constants.INLINKS, referencingUri, seq, link.getAnchorText());
      mutations.add(m);
    }

    for (Link link : deletedLinks) {
      Mutation m = new Mutation("p:" + link.getPageID());
      m.putDelete(Constants.INLINKS, referencingUri, seq);
      mutations.add(m);
    }

    return mutations;
  }

}
