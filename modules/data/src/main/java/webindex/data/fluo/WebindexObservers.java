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

import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter;
import org.apache.fluo.recipes.core.combine.CombineQueue;
import org.apache.fluo.recipes.core.combine.SummingCombiner;
import org.apache.fluo.recipes.core.export.ExportQueue;

import webindex.core.Constants;
import webindex.core.models.UriInfo;
import webindex.core.models.export.IndexUpdate;
import webindex.data.FluoApp;

/**
 * Provides all of the observers needed for this application.
 */
public class WebindexObservers implements ObserverProvider {

  @Override
  public void provide(Registry obsRegistry, Context ctx) {
    SimpleConfiguration appCfg = ctx.getAppConfiguration();
    MetricsReporter reporter = ctx.getMetricsReporter();

    // Create an export queue that handles all updates to the query table.
    ExportQueue<String, IndexUpdate> exportQ =
        ExportQueue.getInstance(FluoApp.EXPORT_QUEUE_ID, appCfg);

    // Create a combineQ that tracks the number of pages linking to a URI.
    CombineQueue<String, UriInfo> uriQ =
        CombineQueue.getInstance(UriCombineQ.URI_COMBINE_Q_ID, appCfg);

    // Create a combineQ that tracks the number of unique URIs observed per domain.
    CombineQueue<String, Long> domainQ =
        CombineQueue.getInstance(DomainCombineQ.DOMAIN_COMBINE_Q_ID, appCfg);

    // Register an observer that handles changes to pages content.
    obsRegistry.forColumn(Constants.PAGE_NEW_COL, NotificationType.STRONG).withId("PageObserver")
        .useObserver(new PageObserver(uriQ, exportQ, reporter));

    // Register an observer to processes queued export data.
    exportQ.registerObserver(obsRegistry, new AccumuloExporter<>(FluoApp.EXPORT_QUEUE_ID, appCfg,
        new IndexUpdateTranslator(reporter)));

    // Register an observer to process updates to the URI map.
    uriQ.registerObserver(obsRegistry, UriInfo::reduce, new UriCombineQ.UriUpdateObserver(exportQ,
        domainQ, reporter));

    // Register an observer to process updates to the domain map.
    domainQ.registerObserver(obsRegistry, new SummingCombiner<>(),
        new DomainCombineQ.DomainUpdateObserver(exportQ, reporter));
  }

}
