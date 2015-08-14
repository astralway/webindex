package io.fluo.commoncrawl.recipes.exportq.accumulo;

import org.apache.accumulo.core.data.Mutation;

import io.fluo.api.observer.Observer.Context;

//TODO a shared batch writer like in phrasecount and fluo degree examples... 
public class SharedBatchWriter {
  static SharedBatchWriter getInstance(Context c) {
    return null;
  }

  public void addAsync(Mutation m) {
    // TODO Auto-generated method stub
    
  }

  public void flush() {
    // TODO Auto-generated method stub
    
  }
}
