package io.fluo.commoncrawl.recipes.serialization;

import com.google.common.base.Charsets;

public class StringSerializer implements SimpleSerializer<String> {

  @Override
  public String deserialize(byte[] b) {
    return new String(b, Charsets.UTF_8);
  }

  @Override
  public byte[] serialize(String e) {
    return e.getBytes(Charsets.UTF_8);
  }
}
