package io.fluo.commoncrawl.data.fluo;

import java.util.Map;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.commoncrawl.core.ColumnConstants;
import io.fluo.commoncrawl.data.util.FluoConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InlinksObserver extends AbstractObserver {

  private static final Logger log = LoggerFactory.getLogger(InlinksObserver.class);

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {

    TypedTransactionBase ttx = FluoConstants.TYPEL.wrap(tx);

    String pageUri = row.toString().substring(2);

    log.debug("Running InLinksObserver for page {}", pageUri);

    ScannerConfiguration scanConf = new ScannerConfiguration();
    scanConf.setSpan(Span.exact(row, new Column(FluoConstants.INLINKS_UPDATE)));

    long change = 0;
    RowIterator rowIter = ttx.get(scanConf);
    if (rowIter.hasNext()) {
      Map.Entry<Bytes, ColumnIterator> rowEntry = rowIter.next();
      ColumnIterator colIter = rowEntry.getValue();
      while (colIter.hasNext()) {
        Map.Entry<Column, Bytes> colEntry = colIter.next();
        String linkUri = colEntry.getKey().getQualifier().toString();
        String update = colEntry.getValue().toString();
        if (update.startsWith("del")) {
          ttx.mutate().row(row).fam(ColumnConstants.INLINKS).qual(linkUri).delete();
          ttx.mutate().row(row).col(colEntry.getKey()).delete();
          change--;
          log.debug("Deleted inlink {} for page {}", linkUri, pageUri);
        } else if (update.startsWith("add")) {
          ttx.mutate().row(row).fam(ColumnConstants.INLINKS).qual(linkUri).set(update.substring(4));
          ttx.mutate().row(row).col(colEntry.getKey()).delete();
          change++;
          log.debug("Added inlink {} for page {}", linkUri, pageUri);
        } else {
          log.error("Unknown update format: {}", update);
        }
      }
    }

    Long incount = ttx.get().row(row).col(FluoConstants.PAGE_INCOUNT_COL).toLong(0) + change;
    if (incount <= 0) {
      if (incount < 0) {
        log.error("Incount for {} is negative: {}", row, incount);
      }
      ttx.mutate().row(row).col(FluoConstants.PAGE_INCOUNT_COL).delete();
    } else {
      ttx.mutate().row(row).col(FluoConstants.PAGE_INCOUNT_COL).set(incount);
    }

    Long score = ttx.get().row(row).col(FluoConstants.PAGE_SCORE_COL).toLong(0) + change;
    if (score <= 0) {
      if (score < 0) {
        log.error("Score for {} is negative: {}", row, incount);
      }
      ttx.mutate().row(row).col(FluoConstants.PAGE_SCORE_COL).delete();
    } else {
      ttx.mutate().row(row).col(FluoConstants.PAGE_SCORE_COL).set(score);
    }

    // TODO - export changes
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(FluoConstants.INLINKS_CHG_NTFY, NotificationType.WEAK);
  }
}
