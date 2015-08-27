package io.fluo.commoncrawl.recipes.exportq.accumulo;

import org.apache.accumulo.core.data.Mutation;

import io.fluo.commoncrawl.recipes.serialization.SimpleSerializer;
import io.fluo.commoncrawl.recipes.serialization.StringSerializer;

public class TestExporter extends AccumuloExporter<String, String> {

  public TestExporter() {
    super(TestExportQueue.QUEUE_ID);
  }

  @Override
  protected Mutation convert(String key, long seq, String value) {
    Mutation m = new Mutation(key);
    m.put("cf", "cq", seq, value);
    return m;
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
