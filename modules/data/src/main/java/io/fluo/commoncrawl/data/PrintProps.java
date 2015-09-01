package io.fluo.commoncrawl.data;

import java.io.File;
import java.util.Iterator;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.commoncrawl.core.DataConfig;
import io.fluo.commoncrawl.data.fluo.IndexExporter;
import io.fluo.recipes.accumulo.export.AccumuloExporter;
import io.fluo.recipes.accumulo.export.TableInfo;
import io.fluo.recipes.export.ExportQueueOptions;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintProps {

  private static final Logger log = LoggerFactory.getLogger(PrintProps.class);

  public static void main(String[] args) {

    if (args.length != 1) {
      log.error("Usage: Init <dataConfigPath>");
      System.exit(1);
    }
    DataConfig dataConfig = DataConfig.load(args[0]);
    FluoConfiguration fluoConfig = new FluoConfiguration(new File(dataConfig.getFluoPropsPath()));

    Configuration exportConfig = new PropertiesConfiguration();
    AccumuloExporter.setExportTableInfo(exportConfig, IndexExporter.QUEUE_ID,
        new TableInfo(fluoConfig.getAccumuloInstance(), fluoConfig.getAccumuloZookeepers(),
            fluoConfig.getAccumuloUser(), fluoConfig.getAccumuloPassword(),
            dataConfig.accumuloIndexTable));
    new IndexExporter().setConfiguration(exportConfig, new ExportQueueOptions(13, 17));

    Iterator iter = exportConfig.getKeys();

    while (iter.hasNext()) {
      String key = (String) iter.next();
      System.out.println(FluoConfiguration.APP_PREFIX + "." + key + " = "
          + exportConfig.getProperty(key));
    }
  }
}
