package io.fluo.commoncrawl.recipes.exportq;


import org.apache.commons.configuration.Configuration;

import com.google.common.hash.Hashing;

import io.fluo.api.client.Transaction;
import io.fluo.commoncrawl.recipes.Encoder;

public abstract class ExportQueue<K, V> {
  private int numBuckets;
  private String queueId;

  
  protected abstract Encoder<K> getKeyEncoder();
  
  protected abstract Encoder<V> getValueEncoder();
  
  //usage hint : could be created once in an observers init method
  //usage hint : maybe have a queue for each type of data being exported???  maybe less queues are more efficient though because more batching at export time??
  protected ExportQueue(String queueId, Configuration appConfig){
    this.queueId = queueId;
    this.numBuckets = appConfig.getInt("recipes.exportQueue."+queueId+".numBuckets");
    if(numBuckets <= 0) {
      throw new IllegalArgumentException("numBuckets is not positive");
    }
  }
  
  public void add(Transaction tx, K key, V value) {
    
    byte[] k = getKeyEncoder().encode(key);
    byte[] v = getValueEncoder().encode(value);
    
    int bucket = Hashing.murmur3_32().hashBytes(k).asInt() % numBuckets;
    
    ExportQueueModel model = new ExportQueueModel(tx);
    
    long seq = model.getSequenceNumber(queueId, bucket);
    
    model.add(queueId, bucket, seq, k, v);
    
    model.setSequenceNumber(queueId, bucket, seq + 1);
    
    model.notifyExportObserver(queueId, bucket, k);
  }
}
