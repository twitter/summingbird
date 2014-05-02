package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;

public class Service<P extends Platform<P>, SERVICE, K, V> extends Wrapper<SERVICE> {

  public Service(SERVICE service) {
    super(service);
  }

}
