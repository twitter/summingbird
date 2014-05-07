package com.twitter.summingbird.storm.javaapi;

import com.twitter.summingbird.javaapi.JProducer;
import com.twitter.summingbird.javaapi.JProducers;
import com.twitter.summingbird.javaapi.Service;
import com.twitter.summingbird.javaapi.Sink;
import com.twitter.summingbird.javaapi.Source;
import com.twitter.summingbird.javaapi.Store;
import com.twitter.summingbird.storm.Storm;
import com.twitter.summingbird.storm.StormService;
import com.twitter.summingbird.storm.StormSink;
import com.twitter.summingbird.storm.StormSource;
import com.twitter.summingbird.storm.StormStore;

public class JStorm {

  public static <T> JProducer<Storm, T> source(StormSource<T> source) {
    return JProducers.<Storm, T>source(new Source<Storm, StormSource<T>, T>(source));
  }

  public static <K,V> Store<Storm, StormStore<K, V>, K, V> store(StormStore<K, V> store) {
    return new Store<Storm, StormStore<K, V>, K, V>(store);
  }

  public static <T> Sink<Storm, StormSink<T>, T> sink(StormSink<T> sink) {
    return new Sink<Storm, StormSink<T>, T>(sink);
  }

  public static <K,V> Service<Storm, StormService<K, V>, K, V> service(StormService<K, V> service) {
    return new Service<Storm, StormService<K, V>, K, V>(service);
  }
}
