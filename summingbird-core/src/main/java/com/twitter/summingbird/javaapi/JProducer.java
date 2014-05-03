package com.twitter.summingbird.javaapi;

import scala.Function1;
import scala.Option;
import scala.util.Either;

import com.twitter.summingbird.Platform;

public interface JProducer<P extends Platform<P>, T> {

  com.twitter.summingbird.Producer<P, T> unwrap();

  JProducer<P, T> name(String id);

  // invariant
  JProducer<P, T> merge(JProducer<P, T> r);

  // collect does not really apply in java

  JProducer<P, T> filter(Function1<T, Boolean> f);

  // invariant
  <V> JKeyedProducer<P, T, Option<V>> lookup(Service<P, ?, T, V> service);

  <U> JProducer<P, U> map(Function1<T, U> f);

  <U> JProducer<P, U> optionMap(Function1<T, Option<U>> f);

  <U> JProducer<P, U> flatMap(Function1<T, Iterable<U>> f);

  // invariant
  JTailProducer<P, T> write(Sink<P, ?, T> sink);

  <U> JProducer<P, Either<T, U>> either(JProducer<P, U> other);

  <K, V> JKeyedProducer<P, K, V> asKeyed();

}