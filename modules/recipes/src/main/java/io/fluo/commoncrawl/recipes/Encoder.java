package io.fluo.commoncrawl.recipes;

//TODO is there something more standard that can be used? Do not want to use Hadoop writable and then depend on hadoop
//TODO avoid object array allocations for repeated use?
public interface Encoder<T> {

  T decode(byte[] b);

  byte[] encode(T e);
}
