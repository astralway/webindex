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

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.metrics.Meter;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.recipes.core.combine.ChangeObserver;
import org.apache.fluo.recipes.core.combine.CombineQueue;
import org.apache.fluo.recipes.core.export.ExportQueue;
import webindex.core.models.export.DomainUpdate;
import webindex.core.models.export.IndexUpdate;

public class DomainCombineQ {

  public static final String DOMAIN_COMBINE_Q_ID = "dm";

  /**
   * Observes domain map updates and adds those updates to an export queue.
   */
  public static class DomainUpdateObserver implements ChangeObserver<String, Long> {

    private ExportQueue<String, IndexUpdate> exportQ;
    private Meter domainsNew;
    private Meter domainsChanged;

    DomainUpdateObserver(ExportQueue<String, IndexUpdate> exportQ, MetricsReporter reporter) {
      this.exportQ = exportQ;
      domainsNew = reporter.meter("webindex_domains_new");
      domainsChanged = reporter.meter("webindex_domains_changed");
    }

    @Override
    public void process(TransactionBase tx, Iterable<Change<String, Long>> updates) {
      for (Change<String, Long> update : updates) {
        String domain = update.getKey();
        Long oldVal = update.getOldValue().orElse(0L);
        Long newVal = update.getNewValue().orElse(0L);
        if (oldVal == 0L && newVal > 0L) {
          domainsNew.mark();
        }
        exportQ.add(tx, domain, new DomainUpdate(domain, oldVal, newVal));
        domainsChanged.mark();
      }
    }
  }

  /**
   * A helper method for configuring the domain map before initializing Fluo.
   */
  public static void configure(FluoConfiguration config, int numBuckets, int numTablets) {
    CombineQueue.configure(DOMAIN_COMBINE_Q_ID).keyType(String.class).valueType(Long.class)
        .buckets(numBuckets).bucketsPerTablet(numBuckets / numTablets).save(config);
  }
}
