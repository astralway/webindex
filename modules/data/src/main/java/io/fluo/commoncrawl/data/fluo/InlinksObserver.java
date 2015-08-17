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

    log.info("Running InLinksObserver for page {}", pageUri);

    ScannerConfiguration scanConf = new ScannerConfiguration();
    scanConf.setSpan(Span.exact(row, new Column(FluoConstants.INLINKS_UPDATE)));

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
          log.info("Deleted inlink {} for page {}", linkUri, pageUri);
        } else if (update.startsWith("add")) {
          ttx.mutate().row(row).fam(ColumnConstants.INLINKS).qual(linkUri).set(update.substring(4));
          ttx.mutate().row(row).col(colEntry.getKey()).delete();
          log.info("Added inlink {} for page {}", linkUri, pageUri);
        } else {
          log.error("Unknown update format: {}", update);
        }
      }
    }

    /* Bytes anchorText = ttx.get(row, col);
    Long inlinkCount = ttx.get().row(linkRow).col(FluoConstants.INLINKCOUNT_COL).toLong(0);

    ttx.mutate().row(linkRow).fam(ColumnConstants.INLINKS).qual(pageUri).set(anchorText);
    ttx.mutate().row(linkRow).col(FluoConstants.INLINKCOUNT_COL).set(inlinkCount+1); */
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(FluoConstants.INLINKS_CHG_NTFY, NotificationType.WEAK);
  }
}
