package com.twitter.summingbird.java;

import com.twitter.summingbird.Platform;

public class Buffer<P extends Platform<P>, BUFFER, K, V> {
  private final BUFFER platformSpecificBuffer;

  public BUFFER unwrap() {
    return platformSpecificBuffer;
  }

  public Buffer(BUFFER platformSpecificBuffer) {
    super();
    this.platformSpecificBuffer = platformSpecificBuffer;
  }

}
