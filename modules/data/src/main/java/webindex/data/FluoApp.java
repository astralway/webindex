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

package webindex.data;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter;
import org.apache.fluo.recipes.core.data.RowHasher;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.kryo.KryoSimplerSerializer;
import webindex.core.models.export.IndexUpdate;
import webindex.data.fluo.DomainMap;
import webindex.data.fluo.PageObserver;
import webindex.data.fluo.UriMap;
import webindex.data.fluo.WebindexObservers;
import webindex.serialization.WebindexKryoFactory;

public class FluoApp {

  public static final String EXPORT_QUEUE_ID = "eq";

  public static void configureApplication(FluoConfiguration fluoConfig, String exportTable,
      int numBuckets, int numTablets) {

    fluoConfig.setObserverProvider(WebindexObservers.class);

    KryoSimplerSerializer.setKryoFactory(fluoConfig, WebindexKryoFactory.class);

    UriMap.configure(fluoConfig, numBuckets, numTablets);
    DomainMap.configure(fluoConfig, numBuckets, numTablets);

    ExportQueue.configure(EXPORT_QUEUE_ID).keyType(String.class).valueType(IndexUpdate.class)
        .buckets(numBuckets).bucketsPerTablet(numBuckets / numTablets).save(fluoConfig);

    AccumuloExporter.configure(EXPORT_QUEUE_ID)
        .instance(fluoConfig.getAccumuloInstance(), fluoConfig.getAccumuloZookeepers())
        .credentials(fluoConfig.getAccumuloUser(), fluoConfig.getAccumuloPassword())
        .table(exportTable).save(fluoConfig);

    RowHasher.configure(fluoConfig, PageObserver.getPageRowHasher().getPrefix(), numTablets);
  }
}
