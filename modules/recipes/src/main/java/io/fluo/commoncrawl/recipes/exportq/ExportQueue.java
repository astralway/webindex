package io.fluo.commoncrawl.recipes.exportq;


import com.google.common.hash.Hashing;
import io.fluo.api.client.TransactionBase;
import io.fluo.commoncrawl.recipes.serialization.SimpleSerializer;

import org.apache.commons.configuration.Configuration;

public abstract class ExportQueue<K, V> {

  private int numBuckets;
  private String queueId;
  private int numCounters;


  // usage hint : could be created once in an observers init method
  // usage hint : maybe have a queue for each type of data being exported??? maybe less queues are
  // more efficient though because more batching at export time??
  protected ExportQueue(String queueId, Configuration appConfig) {
    this.queueId = queueId;
    this.numBuckets = appConfig.getInt("recipes.exportQueue." + queueId + ".buckets");
    if (numBuckets <= 0) {
      throw new IllegalArgumentException("buckets is not positive");
    }

    this.numCounters = appConfig.getInt("recipes.exportQueue." + queueId + ".counters");

    if (numCounters <= 0) {
      throw new IllegalArgumentException("counters is not positive");
    }
  }

  protected abstract SimpleSerializer<K> getKeySerializer();

  protected abstract SimpleSerializer<V> getValueSerializer();

  public static void setConfiguration(Configuration appConfig, String queueId,
      ExportQueueOptions opts) {
    appConfig.setProperty("recipes.exportQueue." + queueId + ".buckets", opts.numBuckets + "");
    appConfig.setProperty("recipes.exportQueue." + queueId + ".counters", opts.numCounters + "");
  }

  public void add(TransactionBase tx, K key, V value) {

    byte[] k = getKeySerializer().serialize(key);
    byte[] v = getValueSerializer().serialize(value);

    int hash = Hashing.murmur3_32().hashBytes(k).asInt();
    int bucket = Math.abs(hash % numBuckets);
    // hash the hash for the case where numBuckets == numCounters... w/o hashing the hash there
    // would only be 1 counter per bucket in this case
    int counter = Math.abs(Hashing.murmur3_32().hashInt(hash).asInt() % numCounters);

    ExportQueueModel model = new ExportQueueModel(tx);

    long seq = model.getSequenceNumber(queueId, bucket, counter);

    model.add(queueId, bucket, seq, k, v);

    model.setSequenceNumber(queueId, bucket, counter, seq + 1);

    model.notifyExportObserver(queueId, bucket, k);
  }
}
