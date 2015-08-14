package io.fluo.commoncrawl.recipes.exportq;

import java.util.Iterator;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.commoncrawl.recipes.Encoder;

public abstract class ExportQueueObserver<K,V> extends AbstractObserver {

  private String exportQueueId;
  
  protected abstract Encoder<K> getKeyEncoder();
  
  protected abstract Encoder<V> getValueEncoder();
  
  protected ExportQueueObserver(String queueId){
    this.exportQueueId = queueId;
  }
  
  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(new Column("fluoRecipes", "exportQ"), NotificationType.WEAK);
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column column) throws Exception {
    ExportQueueModel model = new ExportQueueModel(tx);
    
    int bucket = model.getBucket(row, column);
    
    Iterator<ExportEntry> exportIterator = model.getExportIterator(exportQueueId, bucket);
    
    while(exportIterator.hasNext()) {
      ExportEntry ee = exportIterator.next();
      processExport(getKeyEncoder().decode(ee.key), ee.seq, getValueEncoder().decode(ee.value));
      exportIterator.remove();
    }
  }

  //public void startingToProcessBatch()
  
  /**
   * Must be able to handle same key being exported multiple times and key being exported out of order.   The sequence number is meant to help with this.
   * 
   * @param key
   * @param sequenceNumber
   * @param value
   */
  public abstract void processExport(K key, long sequenceNumber, V value);
  
  //public void finishedProcessingBatch()
  
}
