package com.twitter.summingbird.javaapi;

import scala.Option;
import scala.Tuple2;

import com.twitter.summingbird.Platform;

public interface JProducer<P extends Platform<P>, T> {

  com.twitter.summingbird.Producer<P, T> unwrap();

  JProducer<P, T> name(String id);

  // invariant
  JProducer<P, T> merge(JProducer<P, T> r);

  // collect does not really apply in java < 8

  JProducer<P, T> filter(Predicate<T> f);

  // invariant
  <V> JKeyedProducer<P, T, Option<V>> lookup(Service<P, ?, T, V> service);

  <U> JProducer<P, U> map(Function<T, U> f);

  <U> JProducer<P, U> optionMap(Function<T, Option<U>> f);

  <U> JProducer<P, U> flatMap(Function<T, ? extends Iterable<U>> f);

  // invariant
  JTailProducer<P, T> write(Sink<P, ?, T> sink);

  // Either does not cross compile between scala 2.9 and 2.10 as it moved from scala to scala.util
//  <U> JProducer<P, Either<T, U>> either(JProducer<P, U> other);

  <K, V> JKeyedProducer<P, K, V> mapToKeyed(Function<T, Tuple2<K, V>> f);

}