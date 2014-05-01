package com.twitter.summingbird.java;

import scala.Function1;
import scala.Option;
import scala.util.Either;

import com.twitter.summingbird.Platform;

public interface JProducer<P extends Platform<P>, T> {

  com.twitter.summingbird.Producer<P, T> unwrap();

  JProducer<P, T> name(String id);

  // TODO java does not allow U super T here. Possibly T instead of U?
  <U> JProducer<P, U> merge(JProducer<P, U> r);

  // collect does not really apply in java

  JProducer<P, T> filter(Function1<T, Boolean> f);

  // TODO java does not allow U super T here. used T
  <V> JKeyedProducer<P, T, Option<V>> lookup(Service<P, ?, T, V> service);

  <U> JProducer<P, U> map(Function1<T, U> f);

  <U> JProducer<P, U> optionMap(Function1<T, Option<U>> f);

  <U> JProducer<P, U> flatMap(Function1<T, Iterable<U>> f);

  // TODO java does not allow U super T here. Possibly T instead of U?
  <U> JTailProducer<P, T> write(Sink<P, ?, U> sink);

  <U> JProducer<P, Either<T, U>> either(JProducer<P, U> other);

}