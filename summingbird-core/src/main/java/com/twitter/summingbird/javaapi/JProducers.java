package com.twitter.summingbird.javaapi;

import scala.Tuple2;

import com.twitter.summingbird.Platform;
import com.twitter.summingbird.javaapi.impl.JKeyedProducerImpl;
import com.twitter.summingbird.javaapi.impl.JProducerImpl;

public class JProducers {

  public static <P extends Platform<P>, T> JProducer<P, T> source(Source<P, ?, T> source) {
    return JProducerImpl.source(source);
  }

  public static <P extends Platform<P>, T, K, V> JKeyedProducer<P, K, V> toKeyed(JProducer<P, Tuple2<K, V>> producer) {
    return JKeyedProducerImpl.toKeyed(producer);
  }
}
