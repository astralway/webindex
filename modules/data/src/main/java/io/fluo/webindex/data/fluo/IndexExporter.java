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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.recipes.accumulo.export.AccumuloExporter;
import io.fluo.recipes.transaction.LogEntry;
import io.fluo.recipes.transaction.TxLog;
import io.fluo.webindex.core.Constants;
import io.fluo.webindex.data.spark.IndexUtil;
import io.fluo.webindex.data.util.FluoConstants;
import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexExporter extends AccumuloExporter<Bytes, TxLog> {

  private static final Logger log = LoggerFactory.getLogger(IndexExporter.class);
  public static final String QUEUE_ID = "webIndexQ";

  public static void deleteDomainIndex(Map<Bytes, Mutation> mutations, Bytes row, String pageUri,
      long seq, Long prev) {
    if (prev != null) {
      Mutation m = mutations.computeIfAbsent(row, k -> new Mutation(k.toArray()));
      String cf = String.format("%s:%s", IndexUtil.revEncodeLong(prev), pageUri);
      m.putDelete(Constants.RANK.getBytes(), cf.getBytes(), seq);
      log.debug("Deleted rank index for row {} cf {} seq {}", row.toString(), cf, seq);
    }
  }

  public static void updateDomainIndex(Map<Bytes, Mutation> mutations, Bytes row, String pageUri,
      long seq, Long prev, Long cur) {
    if (!cur.equals(prev)) {
      deleteDomainIndex(mutations, row, pageUri, seq, prev);
      Mutation m = mutations.computeIfAbsent(row, k -> new Mutation(k.toArray()));
      String cf = String.format("%s:%s", IndexUtil.revEncodeLong(cur), pageUri);
      m.put(Constants.RANK.getBytes(), cf.getBytes(), seq, cur.toString().getBytes());
      log.debug("Adding rank index for row {} cf {} seq {} val {}", row.toString(), cf, seq,
          cur.toString());
    }
  }

  public static void deleteTotalIndex(Map<Bytes, Mutation> mutations, String pageUri, long seq,
      Long prev) {
    if (prev != null) {
      Bytes row = Bytes.of(String.format("t:%s:%s", IndexUtil.revEncodeLong(prev), pageUri));
      Mutation m = mutations.computeIfAbsent(row, k -> new Mutation(k.toArray()));
      String cf = String.format("%s:%s", IndexUtil.revEncodeLong(prev), pageUri);
      m.putDelete(Column.EMPTY.getFamily().toArray(), Column.EMPTY.getQualifier().toArray(), seq);
      log.debug("Deleted total index at row {} seq {}", row.toString(), seq);
    }
  }

  public static void updateTotalIndex(Map<Bytes, Mutation> mutations, String pageUri, long seq,
      Long prev, Long cur) {
    if (!cur.equals(prev)) {
      deleteTotalIndex(mutations, pageUri, seq, prev);
      Bytes row = Bytes.of(String.format("t:%s:%s", IndexUtil.revEncodeLong(cur), pageUri));
      Mutation m = mutations.computeIfAbsent(row, k -> new Mutation(k.toArray()));
      m.put(Column.EMPTY.getFamily().toArray(), Column.EMPTY.getQualifier().toArray(), seq, cur
          .toString().getBytes());
      log.debug("Adding total index for row {} seq {} val {}", row.toString(), seq, cur.toString());
    }
  }

  @Override
  protected Collection<Mutation> convert(Bytes key, long seq, TxLog txLog) {
    Map<Bytes, Mutation> mutations = new HashMap<>();

    Map<RowColumn, Bytes> getMap = txLog.getOperationMap(LogEntry.Operation.GET);
    for (LogEntry entry : txLog.getLogEntries()) {
      LogEntry.Operation op = entry.getOp();
      Bytes row = entry.getRow();
      Column col = entry.getColumn();
      Bytes fam = entry.getColumn().getFamily();
      Bytes qual = entry.getColumn().getQualifier();
      Bytes val = entry.getValue();

      log.debug("{} {} row {} col {} val {}", seq, entry.getOp(), entry.getRow(),
          entry.getColumn(), entry.getValue());

      if (op.equals(LogEntry.Operation.DELETE) || op.equals(LogEntry.Operation.SET)) {
        Mutation m = mutations.computeIfAbsent(row, k -> new Mutation(k.toArray()));
        if (entry.getOp().equals(LogEntry.Operation.DELETE)) {
          m.putDelete(fam.toArray(), qual.toArray(), seq);
        } else {
          m.put(fam.toArray(), qual.toArray(), seq, val.toArray());
        }
      }

      if (col.equals(FluoConstants.PAGE_INCOUNT_COL)
          && (op.equals(LogEntry.Operation.SET) || op.equals(LogEntry.Operation.DELETE))) {

        String pageUri = row.toString().substring(2);
        Long prev = null;
        Bytes prevGet = getMap.get(new RowColumn(row, FluoConstants.PAGE_INCOUNT_COL));
        if (prevGet != null) {
          prev = Long.parseLong(prevGet.toString());
        }

        // update domain counts
        Bytes domainRow = PageObserver.getDomainRow(row);
        if (!domainRow.equals(Bytes.EMPTY)) {
          if (op.equals(LogEntry.Operation.SET)) {
            Long cur = Long.parseLong(val.toString());
            updateDomainIndex(mutations, domainRow, pageUri, seq, prev, cur);
          } else {
            deleteDomainIndex(mutations, domainRow, pageUri, seq, prev);
          }
        }

        // update total counts
        if (op.equals(LogEntry.Operation.SET)) {
          Long cur = Long.parseLong(val.toString());
          updateTotalIndex(mutations, pageUri, seq, prev, cur);
        } else {
          deleteTotalIndex(mutations, pageUri, seq, prev);
        }
      }
    }
    return mutations.values();
  }

  public static Predicate<LogEntry> getFilter() {
    return le -> le.getColumn().getFamily().toString().equals(Constants.PAGE)
        || le.getColumn().getFamily().toString().equals(Constants.INLINKS)
        || le.getColumn().equals(FluoConstants.PAGECOUNT_COL);
  }
}
