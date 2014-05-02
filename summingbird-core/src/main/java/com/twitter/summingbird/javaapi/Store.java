package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;

public class Store<P extends Platform<P>, STORE, K, V> extends Wrapper<STORE> {

  public Store(STORE store) {
    super(store);
  }

}
