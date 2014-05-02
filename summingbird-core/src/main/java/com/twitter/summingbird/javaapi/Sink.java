package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;

public class Sink<P extends Platform<P>, SINK, T> extends Wrapper<SINK> {

  public Sink(SINK sink) {
    super(sink);
  }
}
