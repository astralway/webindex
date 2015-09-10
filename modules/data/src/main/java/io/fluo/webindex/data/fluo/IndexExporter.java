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
import java.util.List;

import io.fluo.api.data.Bytes;
import io.fluo.webindex.core.Constants;
import io.fluo.recipes.accumulo.export.AccumuloExporter;
import io.fluo.recipes.serialization.KryoSimplerSerializer;
import io.fluo.recipes.transaction.TxLog;
import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexExporter extends AccumuloExporter<String, TxLog> {

  private static final Logger log = LoggerFactory.getLogger(IndexExporter.class);
  public static final String QUEUE_ID = "peq";

  public IndexExporter() {
    super(QUEUE_ID, String.class, TxLog.class, new KryoSimplerSerializer());
  }

  @Override
  protected List<Mutation> convert(String key, long seq, TxLog txLog) {
    Mutation m = new Mutation(key);
    boolean modified = false;
    for (TxLog.LogEntry entry : txLog.getLogEntries()) {

      Bytes fam = entry.getColumn().getFamily();
      Bytes qual = entry.getColumn().getQualifier();
      Bytes val = entry.getValue();

      if (fam.toString().equals(Constants.PAGE) || fam.toString().equals(Constants.INLINKS)) {
        log.info("{} {} row {} col {} val {}", seq, entry.getType(), entry.getRow(),
            entry.getColumn(), entry.getValue());
        switch (entry.getType()) {
          case DELETE:
            m.putDelete(fam.toArray(), qual.toArray());
          case SET:
            m.put(fam.toArray(), qual.toArray(), val.toArray());
          default:
            break;
        }
        modified = true;
      }
    }
    if (modified) {
      return Collections.singletonList(m);
    }
    return Collections.EMPTY_LIST;
  }
}
