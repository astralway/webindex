package io.fluo.webindex.ui.util;

import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public abstract class Pager {

  private Scanner scanner;
  private int pageSize;
  private Range pageRange;

  public Pager(Scanner scanner, Range pageRange, int pageSize) {
    this.scanner = scanner;
    this.pageRange = pageRange;
    this.pageSize = pageSize;
  }

  public void getPage(Key nextKey) {
    scanner.setRange(new Range(nextKey, pageRange.getEndKey()));
    foundStart(scanner.iterator());
  }

  public void getPage(int pageNum) {
    scanner.setRange(pageRange);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    if (pageNum > 0) {
      long skip = 0;
      while (skip < (pageNum * pageSize)) {
        Map.Entry<Key, Value> entry = iterator.next();
        skip++;
      }
    }
    foundStart(iterator);
  }

  private void foundStart(Iterator<Map.Entry<Key, Value>> iterator) {
    long num = 0;
    while (iterator.hasNext() && (num < (pageSize + 1))) {
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
