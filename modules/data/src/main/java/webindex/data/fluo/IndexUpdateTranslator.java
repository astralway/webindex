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

import java.util.function.Consumer;

import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.api.metrics.Meter;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloTranslator;
import org.apache.fluo.recipes.core.export.SequencedExport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import webindex.core.IndexClient;
import webindex.core.models.export.DomainUpdate;
import webindex.core.models.export.IndexUpdate;
import webindex.core.models.export.PageUpdate;
import webindex.core.models.export.UriUpdate;

public class IndexUpdateTranslator implements AccumuloTranslator<String, IndexUpdate> {

  private static final Logger log = LoggerFactory.getLogger(IndexUpdateTranslator.class);

  private Meter pagesExported;
  private Meter linksExported;
  private Meter domainsExported;

  public IndexUpdateTranslator(MetricsReporter reporter) {
    pagesExported = reporter.meter("webindex_pages_exported");
    linksExported = reporter.meter("webindex_links_exported");
    domainsExported = reporter.meter("webindex_domains_exported");
  }

  @Override
  public void translate(SequencedExport<String, IndexUpdate> export, Consumer<Mutation> consumer) {
    if (export.getValue() instanceof DomainUpdate) {
      domainsExported.mark();
      IndexClient.genDomainMutations((DomainUpdate) export.getValue(), export.getSequence(),
          consumer);
    } else if (export.getValue() instanceof PageUpdate) {
      pagesExported.mark();
      IndexClient.genPageMutations((PageUpdate) export.getValue(), export.getSequence(), consumer);
    } else if (export.getValue() instanceof UriUpdate) {
      linksExported.mark();
      IndexClient.genUriMutations((UriUpdate) export.getValue(), export.getSequence(), consumer);
    } else {
      String msg =
          "An object with an IndexUpdate class (" + export.getValue().getClass().toString()
              + ") was placed on the export queue";
      log.error(msg);
      throw new IllegalStateException(msg);
    }
  }
}
