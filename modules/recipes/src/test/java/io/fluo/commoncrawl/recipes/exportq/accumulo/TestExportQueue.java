package io.fluo.commoncrawl.recipes.exportq.accumulo;

import org.apache.commons.configuration.Configuration;

import io.fluo.commoncrawl.recipes.exportq.ExportQueue;
import io.fluo.commoncrawl.recipes.serialization.SimpleSerializer;
import io.fluo.commoncrawl.recipes.serialization.StringSerializer;

public class TestExportQueue extends ExportQueue<String,String>{

  public static final String QUEUE_ID = "aeqt";
  
  protected TestExportQueue(Configuration appConfig) {
    super(QUEUE_ID, appConfig);
  }

  @Override
  protected SimpleSerializer<String> getKeySerializer() {
    return new StringSerializer();
  }

  @Override
  protected SimpleSerializer<String> getValueSerializer() {
    return new StringSerializer();
  }

}
