package io.fluo.commoncrawl.recipes.exportq.accumulo;

import org.apache.accumulo.core.data.Mutation;

import io.fluo.commoncrawl.recipes.serialization.StringSerializer;

public class TestExporter extends AccumuloExporter<String, String> {

  public static final String QUEUE_ID = "aeqt";

  public TestExporter() {
    super(QUEUE_ID, new StringSerializer(), new StringSerializer());
  }

  @Override
  protected Mutation convert(String key, long seq, String value) {
    Mutation m = new Mutation(key);
    m.put("cf", "cq", seq, value);
    return m;
  }
}
