package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;

public class Source<P extends Platform<P>, SOURCE, T> extends Wrapper<SOURCE> {

  public Source(SOURCE source) {
    super(source);
  }

}
