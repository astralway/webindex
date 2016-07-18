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

package io.fluo.webindex.data;

import io.fluo.webindex.data.fluo.DomainMap;
import io.fluo.webindex.data.fluo.PageObserver;
import io.fluo.webindex.data.fluo.UriMap;
import io.fluo.webindex.serialization.WebindexKryoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverConfiguration;
import org.apache.fluo.recipes.accumulo.export.AccumuloExport;
import org.apache.fluo.recipes.accumulo.export.AccumuloExporter;
import org.apache.fluo.recipes.accumulo.export.TableInfo;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.kryo.KryoSimplerSerializer;

public class FluoApp {

  public static final String EXPORT_QUEUE_ID = "eq";

  public static void configureApplication(FluoConfiguration appConfig, TableInfo exportTable,
      int numBuckets, int numTablets) {

    appConfig.addObserver(new ObserverConfiguration(PageObserver.class.getName()));

    KryoSimplerSerializer.setKryoFactory(appConfig, WebindexKryoFactory.class);

    UriMap.configure(appConfig, numBuckets, numTablets);
    DomainMap.configure(appConfig, numBuckets, numTablets);

    ExportQueue.configure(appConfig, new ExportQueue.Options(EXPORT_QUEUE_ID,
        AccumuloExporter.class.getName(), String.class.getName(), AccumuloExport.class.getName(),
        numBuckets).setBucketsPerTablet(numBuckets / numTablets));

    AccumuloExporter.setExportTableInfo(appConfig, EXPORT_QUEUE_ID, exportTable);
  }

}
