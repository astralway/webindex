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

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Sets.SetView;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.models.Page;
import io.fluo.webindex.core.models.Page.Link;
import io.fluo.webindex.data.recipes.Transmutable;
import org.apache.accumulo.core.data.Mutation;

public class PageExport implements Transmutable<String> {

  private String json;
  private List<Page.Link> addedLinks;
  private List<Page.Link> deletedLinks;

  public PageExport() {}

  public PageExport(String json, SetView<Page.Link> addedLinks, SetView<Page.Link> deletedLinks) {
    this.json = json;
    this.addedLinks = new ArrayList<>(addedLinks);
    this.deletedLinks = new ArrayList<>(deletedLinks);
  }

  private String getUri(Page.Link link) {
    try {
      // TODO does this need to throw exception???
      return link.getUri();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
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
      Mutation m = new Mutation("p:" + getUri(link));
      m.put(Constants.INLINKS, referencingUri, seq, link.getAnchorText());
      mutations.add(m);
    }

    for (Link link : deletedLinks) {
      Mutation m = new Mutation("p:" + getUri(link));
      m.putDelete(Constants.INLINKS, referencingUri, seq);
      mutations.add(m);
    }

    return mutations;
  }

}
