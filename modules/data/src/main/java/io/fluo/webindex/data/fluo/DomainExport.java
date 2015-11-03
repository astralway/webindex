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

import java.util.Collection;
import java.util.Collections;

import io.fluo.webindex.core.Constants;
import io.fluo.webindex.data.recipes.Transmutable;
import org.apache.accumulo.core.data.Mutation;

public class DomainExport implements Transmutable<String> {
  private long count;

  public DomainExport() {}

  public DomainExport(long c) {
    this.count = c;
  }

  @Override
  public Collection<Mutation> toMutations(String domain, long seq) {
    Mutation m = new Mutation("d:" + domain);
    if (count == 0) {
      m.putDelete(Constants.DOMAIN, Constants.PAGECOUNT, seq);
    } else {
      m.put(Constants.DOMAIN, Constants.PAGECOUNT, seq, count + "");
    }
    return Collections.singleton(m);
  }
}
