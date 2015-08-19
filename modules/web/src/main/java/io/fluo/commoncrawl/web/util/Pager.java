package io.fluo.commoncrawl.web.util;

import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public abstract class Pager {

  private Scanner scanner;
  private String row;
  private String cf;
  private String nextCq;
  private int pageNum;
  private int pageSize;

  public Pager(Scanner scanner, String row, String cf, String nextCq, int pageNum, int pageSize) {
    this.scanner = scanner;
    this.row = row;
    this.cf = cf;
    this.nextCq = nextCq;
    this.pageNum = pageNum;
    this.pageSize = pageSize;
  }

  public void getPage() {
    if (nextCq.isEmpty()) {
      scanner.setRange(Range.exact(row, cf));
    } else {
      scanner.setRange(new Range(new Key(row, cf, nextCq),
                                 new Key(row, cf).followingKey(PartialKey.ROW_COLFAM)));
    }
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    if (nextCq.isEmpty() && (pageNum > 0)) {
      long skip = 0;
      while (skip < (pageNum*pageSize)) {
        Map.Entry<Key, Value> entry = iterator.next();
        skip++;
      }
    }
    long num = 0;
    while (iterator.hasNext() && (num < (pageSize+1))) {
      Map.Entry<Key, Value> entry = iterator.next();
      if (num == pageSize) {
        foundNextEntry(entry);
      } else {
        foundPageEntry(entry);
      }
      num++;
    }
  }

  public abstract void foundPageEntry(Map.Entry<Key, Value> entry);

  public abstract void foundNextEntry(Map.Entry<Key, Value> entry);

}
