package io.fluo.commoncrawl.recipes.exportq;

import com.google.common.base.Preconditions;

public class ExportQueueOptions {
  int numBuckets;
  int numCounters;
  
  public ExportQueueOptions(int buckets, int counters){
    Preconditions.checkArgument(buckets > 0);
    Preconditions.checkArgument(counters > 0);
    
    this.numBuckets = buckets;
    this.numCounters = counters;
  }
}