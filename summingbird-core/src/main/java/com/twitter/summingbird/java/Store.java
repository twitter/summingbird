package com.twitter.summingbird.java;

import com.twitter.summingbird.Platform;

public class Store<P extends Platform<P>, STORE, K, V> {

  private final STORE platformSpecificStore;

  public STORE unwrap() {
    return platformSpecificStore;
  }

  public Store(STORE store) {
    platformSpecificStore = store;
  }
}
