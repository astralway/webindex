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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.recipes.accumulo.export.DifferenceExport;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.core.models.URL;
import io.fluo.webindex.data.fluo.UriMap.UriInfo;
import io.fluo.webindex.data.util.FluoConstants;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.client.lexicoder.ULongLexicoder;
import org.apache.commons.codec.binary.Hex;

public class UriCountExport extends DifferenceExport<String, UriInfo> {

  public UriCountExport() {}

  public UriCountExport(Optional<UriInfo> oldCount, Optional<UriInfo> newCount) {
    super(oldCount, newCount);
  }

  @Override
  protected Map<RowColumn, Bytes> generateData(String pageID, Optional<UriInfo> val) {
    if (val.orElse(UriInfo.ZERO).equals(UriInfo.ZERO)) {
      return Collections.emptyMap();
    }

    UriInfo uriInfo = val.get();

    Map<RowColumn, Bytes> rcMap = new HashMap<>();
    Bytes linksTo = Bytes.of("" + uriInfo.linksTo);
    rcMap.put(new RowColumn(createTotalRow(pageID, uriInfo.linksTo), Column.EMPTY), linksTo);
    String domain = URL.fromPageID(pageID).getReverseDomain();
    String cq = revEncodeLong(uriInfo.linksTo) + ":" + pageID;
    rcMap.put(new RowColumn("d:" + domain, new Column(Constants.RANK, cq)), linksTo);
    rcMap.put(new RowColumn("p:" + pageID, FluoConstants.PAGE_INCOUNT_COL), linksTo);
    return rcMap;
  }

  public static String revEncodeLong(Long num) {
    Lexicoder<Long> lexicoder = new ReverseLexicoder<>(new ULongLexicoder());
    return Hex.encodeHexString(lexicoder.encode(num));
  }

  private static String createTotalRow(String uri, long curr) {
    return "t:" + revEncodeLong(curr) + ":" + uri;
  }
}
