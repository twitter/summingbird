package com.twitter.summingbird.java;

import com.twitter.summingbird.Platform;

public class Source<P extends Platform<P>, SOURCE, T> {

  private final SOURCE platformSpecificSource;

  public SOURCE unwrap() {
    return platformSpecificSource;
  }

  public Source(SOURCE source) {
    this.platformSpecificSource = source;
  }

}
