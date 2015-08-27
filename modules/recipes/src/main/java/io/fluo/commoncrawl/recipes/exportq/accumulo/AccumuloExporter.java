package io.fluo.commoncrawl.recipes.exportq.accumulo;

import io.fluo.commoncrawl.recipes.exportq.Exporter;

import java.util.ArrayList;

import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.configuration.Configuration;

// TODO ideally this would eventually be in seperate recipe sub module... would not want fluo
// recipes to depend on Accumulo
public abstract class AccumuloExporter<K, V> extends Exporter<K, V> {

  private SharedBatchWriter sbw;
  private ArrayList<Mutation> buffer = new ArrayList<>();
  private long bufferSize = 0;

  protected AccumuloExporter(String queueId) {
    super(queueId);
  }

  @Override
  public void init(Context context) throws Exception {
    super.init(context);

    Configuration appConf = context.getAppConfiguration();

    String instanceName =
        appConf.getString("recipes.accumuloExporter." + getQueueId() + ".instance");
    String zookeepers =
        appConf.getString("recipes.accumuloExporter." + getQueueId() + ".zookeepers");
    String user = appConf.getString("recipes.accumuloExporter." + getQueueId() + ".user");
    // TODO look into using delegation token
    String password = appConf.getString("recipes.accumuloExporter." + getQueueId() + ".password");
    String table = appConf.getString("recipes.accumuloExporter." + getQueueId() + ".table");

    sbw = SharedBatchWriter.getInstance(instanceName, zookeepers, user, password, table);
  }

  public static void setExportTableInfo(Configuration appConf, String queueId, TableInfo ti) {
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".instance", ti.instanceName);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".zookeepers", ti.zookeepers);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".user", ti.user);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".password", ti.password);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".table", ti.table);
  }

  protected abstract Mutation convert(K key, long seq, V value);

  @Override
  protected void startingToProcessBatch() {
    buffer.clear();
    bufferSize = 0;
  }

  @Override
  public void processExport(K key, long sequenceNumber, V value) {
    Mutation m = convert(key, sequenceNumber, value);
    buffer.add(m);
    bufferSize += m.estimatedMemoryUsed();

    if (bufferSize > 1 << 20) {
      finishedProcessingBatch();
    }
  }

  protected void finishedProcessingBatch() {
    if (buffer.size() > 0) {
      sbw.write(buffer);
      buffer.clear();
      bufferSize = 0;
    }
  }
}
