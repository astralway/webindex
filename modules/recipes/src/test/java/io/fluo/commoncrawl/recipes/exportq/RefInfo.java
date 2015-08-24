package io.fluo.commoncrawl.recipes.exportq;

class RefInfo {
  long seq;
  boolean deleted;
  
  public RefInfo(long seq, boolean deleted) {
    this.seq = seq;
    this.deleted = deleted;
  }
}