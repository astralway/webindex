package io.fluo.commoncrawl.recipes.exportq;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.commoncrawl.recipes.serialization.SimpleSerializer;

public abstract class Exporter<K, V> extends AbstractObserver {

  private String queueId;
  private SimpleSerializer<K> keySerializer;
  private SimpleSerializer<V> valueSerializer;

  protected Exporter(String queueId, SimpleSerializer<K> keySerializer,
      SimpleSerializer<V> valueSerializer) {
    this.queueId = queueId;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  protected String getQueueId() {
    return queueId;
  }

  SimpleSerializer<K> getKeySerializer() {
    return keySerializer;
  }

  SimpleSerializer<V> getValueSerializer() {
    return valueSerializer;
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(new Column("fluoRecipes", "eq:" + queueId), NotificationType.WEAK);
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column column) throws Exception {
    ExportQueueModel model = new ExportQueueModel(tx);

    int bucket = model.getBucket(row, column);

    Iterator<ExportEntry> exportIterator = model.getExportIterator(queueId, bucket);

    startingToProcessBatch();

    while (exportIterator.hasNext()) {
      ExportEntry ee = exportIterator.next();
      processExport(keySerializer.deserialize(ee.key), ee.seq,
          valueSerializer.deserialize(ee.value));
      exportIterator.remove();
    }

    finishedProcessingBatch();
  }

  protected void startingToProcessBatch() {}

  /**
   * Must be able to handle same key being exported multiple times and key being exported out of
   * order. The sequence number is meant to help with this.
   */
  protected abstract void processExport(K key, long sequenceNumber, V value);

  protected void finishedProcessingBatch() {}

  /**
   * Can call in the init method of an observer
   * 
   * @param appConfig
   * @return
   */
  public ExportQueue<K, V> getExportQueue(Configuration appConfig) {
    return new ExportQueue<K, V>(appConfig, this);
  }

  public void setConfiguration(Configuration appConfig, ExportQueueOptions opts) {
    appConfig.setProperty("recipes.exportQueue." + getQueueId() + ".buckets", opts.numBuckets + "");
    appConfig.setProperty("recipes.exportQueue." + getQueueId() + ".counters", opts.numCounters
        + "");
  }
}
