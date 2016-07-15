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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import io.fluo.webindex.core.models.URL;
import io.fluo.webindex.data.FluoApp;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.observer.Observer.Context;
import org.apache.fluo.recipes.accumulo.export.AccumuloExport;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.core.map.CollisionFreeMap;
import org.apache.fluo.recipes.core.map.CollisionFreeMap.Options;
import org.apache.fluo.recipes.core.map.Combiner;
import org.apache.fluo.recipes.core.map.Update;
import org.apache.fluo.recipes.core.map.UpdateObserver;

/**
 * This class contains code related to a CollisionFreeMap that keeps track of the count of
 * information about URIs.
 */
public class UriMap {

  public static final String URI_MAP_ID = "um";

  public static class UriInfo implements Serializable {

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
  }

  /**
   * Combines updates made to the uri map
   */
  public static class UriCombiner implements Combiner<String, UriInfo> {
    @Override
    public Optional<UriInfo> combine(String key, Iterator<UriInfo> updates) {

      UriInfo total = new UriInfo(0, 0);

      while (updates.hasNext()) {
        total.add(updates.next());
      }

      if (total.equals(UriInfo.ZERO)) {
        return Optional.empty();
      } else {
        return Optional.of(total);
      }
    }
  }

  /**
   * Observes uri map updates and adds those updates to an export queue.
   */
  public static class UriUpdateObserver extends UpdateObserver<String, UriInfo> {

    private ExportQueue<String, AccumuloExport<String>> exportQ;
    private CollisionFreeMap<String, Long> domainMap;

    @Override
    public void init(String mapId, Context observerContext) throws Exception {
      exportQ =
          ExportQueue.getInstance(FluoApp.EXPORT_QUEUE_ID, observerContext.getAppConfiguration());
      domainMap =
          CollisionFreeMap.getInstance(DomainMap.DOMAIN_MAP_ID,
              observerContext.getAppConfiguration());
    }

    @Override
    public void updatingValues(TransactionBase tx, Iterator<Update<String, UriInfo>> updates) {
      Map<String, Long> domainUpdates = new HashMap<>();

      while (updates.hasNext()) {
        Update<String, UriInfo> update = updates.next();

        UriInfo oldVal = update.getOldValue().orElse(UriInfo.ZERO);
        UriInfo newVal = update.getNewValue().orElse(UriInfo.ZERO);

        exportQ.add(tx, update.getKey(),
            new UriCountExport(update.getOldValue(), update.getNewValue()));

        String pageDomain = URL.fromPageID(update.getKey()).getReverseDomain();
        if (oldVal.equals(UriInfo.ZERO) && !newVal.equals(UriInfo.ZERO)) {
          domainUpdates.merge(pageDomain, 1L, (o, n) -> o + n);
        } else if (newVal.equals(UriInfo.ZERO) && !oldVal.equals(UriInfo.ZERO)) {
          domainUpdates.merge(pageDomain, -1L, (o, n) -> o + n);
        }
      }

      domainMap.update(tx, domainUpdates);
    }
  }

  /**
   * A helper method for configuring the uri map before initializing Fluo.
   *
   */
  public static void configure(FluoConfiguration config, int numBuckets, int numTablets) {
    CollisionFreeMap.configure(config, new Options(URI_MAP_ID, UriCombiner.class,
        UriUpdateObserver.class, String.class, UriInfo.class, numBuckets)
        .setBucketsPerTablet(numBuckets / numTablets));
  }
}
