/*
 * Copyright 2015 Webindex authors (see AUTHORS)
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

package webindex.data.fluo;

import java.util.HashMap;
import java.util.Map;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.metrics.Meter;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.recipes.core.combine.ChangeObserver;
import org.apache.fluo.recipes.core.combine.CombineQueue;
import org.apache.fluo.recipes.core.export.ExportQueue;
import webindex.core.models.URL;
import webindex.core.models.UriInfo;
import webindex.core.models.export.IndexUpdate;
import webindex.core.models.export.UriUpdate;

/**
 * This class contains code related to a CombineQueue that keeps track of the count of information
 * about URIs.
 */
public class UriCombineQ {

  public static final String URI_COMBINE_Q_ID = "um";

  /**
   * Observes uri map updates and adds those updates to an export queue.
   */
  public static class UriUpdateObserver implements ChangeObserver<String, UriInfo> {

    private ExportQueue<String, IndexUpdate> exportQ;
    private CombineQueue<String, Long> domainQ;
    private Meter linksNew;
    private Meter linksChanged;

    public UriUpdateObserver(ExportQueue<String, IndexUpdate> exportQ,
        CombineQueue<String, Long> domainMap, MetricsReporter metricsReporter) {
      this.exportQ = exportQ;
      this.domainQ = domainMap;
      linksNew = metricsReporter.meter("webindex_links_new");
      linksChanged = metricsReporter.meter("webindex_links_changed");
    }

    @Override
    public void process(TransactionBase tx, Iterable<Change<String, UriInfo>> updates) {

      Map<String, Long> domainUpdates = new HashMap<>();

      for (Change<String, UriInfo> update : updates) {
        String uri = update.getKey();
        UriInfo oldVal = update.getOldValue().orElse(UriInfo.ZERO);
        UriInfo newVal = update.getNewValue().orElse(UriInfo.ZERO);

        exportQ.add(tx, uri, new UriUpdate(uri, oldVal, newVal));
        linksChanged.mark();

        String pageDomain = URL.fromUri(uri).getReverseDomain();
        if (oldVal.equals(UriInfo.ZERO) && !newVal.equals(UriInfo.ZERO)) {
          domainUpdates.merge(pageDomain, 1L, (o, n) -> o + n);
          linksNew.mark();
        } else if (newVal.equals(UriInfo.ZERO) && !oldVal.equals(UriInfo.ZERO)) {
          domainUpdates.merge(pageDomain, -1L, (o, n) -> o + n);
        }
      }

      domainQ.addAll(tx, domainUpdates);
    }
  }

  /**
   * A helper method for configuring the uri map before initializing Fluo.
   */
  public static void configure(FluoConfiguration config, int numBuckets, int numTablets) {
    CombineQueue.configure(URI_COMBINE_Q_ID).keyType(String.class).valueType(UriInfo.class)
        .buckets(numBuckets).bucketsPerTablet(numBuckets / numTablets).save(config);
  }
}
