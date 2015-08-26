package io.fluo.commoncrawl.recipes.exportq;

import org.apache.commons.configuration.Configuration;

import io.fluo.commoncrawl.recipes.serialization.SimpleSerializer;
import io.fluo.commoncrawl.recipes.serialization.StringSerializer;

class RefExportQueue extends ExportQueue<String, RefUpdates> {
  public static final String QUEUE_ID = "req";

  RefExportQueue(Configuration config){
    super(QUEUE_ID, config);
  }
  
  @Override
  protected SimpleSerializer<String> getKeySerializer() {
    return new StringSerializer();
  }

  @Override
  protected SimpleSerializer<RefUpdates> getValueSerializer() {
    return RefUpdates.newSerializer();
  }
  
}