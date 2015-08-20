package io.fluo.commoncrawl.recipes.exportq.accumulo;

import io.fluo.commoncrawl.recipes.exportq.Exporter;
import org.apache.accumulo.core.data.Mutation;

//TODO ideally this would eventually be in seperate recipe sub module... would not want fluo recipes to depend on Accumulo
public abstract class AccumuloExporter<K, V> extends Exporter<K, V> {

  private SharedBatchWriter sbw;

  protected AccumuloExporter(String queueId) {
    super(queueId);
  }

  @Override
  public void init(Context context) throws Exception {
    super.init(context);
    sbw = SharedBatchWriter.getInstance(context);

  }

  protected abstract Mutation convert(K key, long seq, V value);

  @Override
  public void processExport(K key, long sequenceNumber, V value) {
    Mutation m = convert(key, sequenceNumber, value);
    //non blocking add
    sbw.addAsync(m);
  }

  protected void finishedProcessingBatch() {
    //should block until everything is flushed
    sbw.flush();
  }


}
