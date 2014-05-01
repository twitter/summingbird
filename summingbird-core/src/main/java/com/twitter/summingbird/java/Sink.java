package com.twitter.summingbird.java;

import com.twitter.summingbird.Platform;

public class Sink<P extends Platform<P>, SINK, T> {

  private final SINK platformSpecificSink;

  public SINK unwrap() {
    return platformSpecificSink;
  }

  public Sink(SINK sink) {
    platformSpecificSink = sink;
  }
}
