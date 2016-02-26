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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.RowColumn;
import io.fluo.recipes.accumulo.export.DifferenceExport;

import static io.fluo.webindex.data.util.FluoConstants.PAGECOUNT_COL;

public class DomainExport extends DifferenceExport<String, Long> {

  public DomainExport() {}

  public DomainExport(Optional<Long> oldCount, Optional<Long> newCount) {
    super(oldCount, newCount);
  }

  @Override
  protected Map<RowColumn, Bytes> generateData(String domain, Optional<Long> count) {
    if (count.orElse(0L) == 0) {
      return Collections.emptyMap();
    }
    return Collections.singletonMap(new RowColumn("d:" + domain, PAGECOUNT_COL),
        Bytes.of(count.get() + ""));
  }
}
