package io.fluo.commoncrawl.data.spark;

import java.io.Serializable;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexStats implements Serializable {

  private static final Logger log = LoggerFactory.getLogger(IndexUtil.class);

  private Accumulator<Integer> numPages;
  private Accumulator<Integer> numEmpty;
  private Accumulator<Integer> numExternalLinks;

  public IndexStats(JavaSparkContext ctx) {
    numPages = ctx.accumulator(0);
    numEmpty = ctx.accumulator(0);
    numExternalLinks = ctx.accumulator(0);
  }

  public void addPage(Integer num) {
    numPages.add(num);
  }

  public void addEmpty(Integer num) {
    numEmpty.add(num);
  }

  public void addExternalLinks(Integer num) {
    numExternalLinks.add(num);
  }

  public void print() {
    log.info("Num empty = {}", numEmpty.value());
    log.info("Num pages = {}", numPages.value());
    log.info("Num external links = {}", numExternalLinks.value());
  }
}
