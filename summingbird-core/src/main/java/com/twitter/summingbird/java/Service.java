package com.twitter.summingbird.java;

import com.twitter.summingbird.Platform;

public class Service<P extends Platform<P>, SERVICE, K, V> {

  private final SERVICE platformSpecificService;

  public SERVICE unwrap() {
    return platformSpecificService;
  }

  public Service(SERVICE service) {
    platformSpecificService = service;
  }

}
