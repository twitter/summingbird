package com.twitter.summingbird.javaapi;

import scala.Tuple2;

public final class KeyValue<K, V> {

  private final Tuple2<K, V> t;

  public KeyValue(K key, V value) {
    super();
    this.t = new Tuple2<K, V>(key, value);
  }

  public K getKey() {
    return t._1;
  }

  public V getValue() {
    return t._2;
  }

  public Tuple2<K, V> unwrap() {
    return t;
  }

}
