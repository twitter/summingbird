package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;

public class Buffer<P extends Platform<P>, BUFFER, K, V> extends Wrapper<BUFFER> {

  public Buffer(BUFFER buffer) {
    super(buffer);
  }

}
