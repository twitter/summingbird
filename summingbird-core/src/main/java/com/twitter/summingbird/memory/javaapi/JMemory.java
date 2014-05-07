package com.twitter.summingbird.memory.javaapi;

import java.util.List;
import java.util.Map;

import scala.Function1;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.TraversableOnce;
import scala.runtime.AbstractFunction1;

import com.twitter.summingbird.javaapi.Function;
import com.twitter.summingbird.javaapi.JProducer;
import com.twitter.summingbird.javaapi.JProducers;
import com.twitter.summingbird.javaapi.JTailProducer;
import com.twitter.summingbird.javaapi.Service;
import com.twitter.summingbird.javaapi.Sink;
import com.twitter.summingbird.javaapi.Source;
import com.twitter.summingbird.javaapi.Store;
import com.twitter.summingbird.javaapi.impl.JProducerImpl;
import com.twitter.summingbird.memory.Memory;

public class JMemory {
  private static <IN> Function1<IN, Void> toScala(final JSink<IN> f) {
    return new AbstractFunction1<IN, Void>() {
      @Override
      public Void apply(IN v) {
        f.write(v);
        return null;
      }
    };
  }

  public static <T> JProducer<Memory, T> source(Iterable<T> source) {
    return JProducers.source(new Source<Memory, TraversableOnce<T>, T>(JavaConversions.iterableAsScalaIterable(source)));
  }

  public static <K,V> Store<Memory, scala.collection.mutable.Map<K, V>, K, V> store(Map<K, V> store) {
    return new Store<Memory, scala.collection.mutable.Map<K, V>, K, V>(JavaConversions.asScalaMap(store));
  }

  public static <T> Sink<Memory, Function1<T, Void>, T> sink(JSink<T> sink) {
    return new Sink<Memory, Function1<T, Void>, T>(toScala(sink));
  }

  public static <K,V> Service<Memory, Function1<K, Option<V>>, K, V> service(Function<K, Option<V>> service) {
    return new Service<Memory, Function1<K, Option<V>>, K, V>(JProducerImpl.toScala(service));
  }

  private Memory platform = new Memory();

  public <T> List<T> plan(JTailProducer<Memory, T> tail) {
	  return JavaConversions.asJavaList(platform.plan(tail.unwrap()));
  }

}
