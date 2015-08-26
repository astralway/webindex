package io.fluo.commoncrawl.recipes.serialization;

//TODO is there something more standard that can be used? Do not want to use Hadoop writable and then depend on hadoop
//TODO avoid object array allocations for repeated use?
public interface SimpleSerializer<T> {
  //TODO use data input/output
  T deserialize(byte[] b);
  byte[] serialize(T e);
}
