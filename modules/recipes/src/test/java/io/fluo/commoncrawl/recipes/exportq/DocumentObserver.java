package io.fluo.commoncrawl.recipes.exportq;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.TypedObserver;
import io.fluo.api.types.TypedTransactionBase;

public class DocumentObserver extends TypedObserver {

  ExportQueue<String, RefUpdates> refExportQueue;

  @Override
  public void init(Context context) throws Exception {
    refExportQueue = new ExportQueueIT.RefExporter().getExportQueue(context.getAppConfiguration());
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(new Column("content", "new"), NotificationType.STRONG);
  }

  @Override
  public void process(TypedTransactionBase tx, Bytes row, Column col) {
    String newContent = tx.get().row(row).col(col).toString();
    Set<String> newRefs = new HashSet<>(Arrays.asList(newContent.split(" ")));
    Set<String> currentRefs =
        new HashSet<>(Arrays.asList(tx.get().row(row).fam("content").qual("current").toString("")
            .split(" ")));

    Set<String> addedRefs = new HashSet<>(newRefs);
    addedRefs.removeAll(currentRefs);

    Set<String> deletedRefs = new HashSet<>(currentRefs);
    deletedRefs.removeAll(newRefs);

    String key = row.toString().substring(2);
    RefUpdates val = new RefUpdates(addedRefs, deletedRefs);

    refExportQueue.add(tx, key, val);

    tx.mutate().row(row).fam("content").qual("current").set(newContent);
  }
}
