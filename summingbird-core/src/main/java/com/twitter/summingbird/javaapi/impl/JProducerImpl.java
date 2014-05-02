package com.twitter.summingbird.javaapi.impl;

import com.twitter.summingbird.KeyedProducer;
import com.twitter.summingbird.Platform;
import com.twitter.summingbird.Producer;
import com.twitter.summingbird.Summer;
import com.twitter.summingbird.TailProducer;

import com.twitter.summingbird.Producer$;
import com.twitter.summingbird.javaapi.JKeyedProducer;
import com.twitter.summingbird.javaapi.JProducer;
import com.twitter.summingbird.javaapi.JSummer;
import com.twitter.summingbird.javaapi.JTailProducer;
import com.twitter.summingbird.javaapi.Service;
import com.twitter.summingbird.javaapi.Sink;
import com.twitter.summingbird.javaapi.Source;

import scala.Function1;
import scala.Tuple2;
import scala.Option;
import scala.runtime.AbstractFunction1;
import scala.util.Either;
import scala.collection.TraversableOnce;
import scala.collection.JavaConversions;

public class JProducerImpl<P extends Platform<P>, T> implements JProducer<P, T> {

  public static <P extends Platform<P>, T> JProducer<P, T> source(Source<P, ?, T> source) {
    return new JProducerImpl<P, T>(Producer$.MODULE$.<P, T>source(source.unwrap()));
  }

  static <IN, OUT> Function1<IN, TraversableOnce<OUT>> toTraversableOnce(final Function1<IN, Iterable<OUT>> f) {
    return new AbstractFunction1<IN, TraversableOnce<OUT>>() {
      public TraversableOnce<OUT> apply(IN v) {
        return JavaConversions.iterableAsScalaIterable(f.apply(v));
      }
    };
  }

  @SuppressWarnings("unchecked")
  static <PARAM> Function1<PARAM, Object> eraseReturnType(Function1<PARAM, ?> f) {
    return (Function1<PARAM, Object>)f;
  }

  com.twitter.summingbird.Producer<P, T> delegate;

  JProducerImpl(Producer<P, T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Producer<P, T> unwrap() {
    return delegate;
  }

  static <P extends Platform<P>, U> JProducer<P, U> wrap(Producer<P, U> delegate) {
    return new JProducerImpl<P, U>(delegate);
  }

  static <P extends Platform<P>, K, V> JKeyedProducer<P, K, V> wrap(KeyedProducer<P, K, V> delegate) {
    return new JKeyedProducerImpl<P, K, V>(delegate);
  }

  static <P extends Platform<P>, U> JTailProducer<P, U> wrap(TailProducer<P, U> delegate) {
    return new JTailProducerImpl<P, U>(delegate);
  }

  static <P extends Platform<P>, K, V> JSummer<P, K, V> wrap(Summer<P, K, V> delegate) {
    return new JSummerImpl<P, K, V>(delegate);
  }

  @Override
  public JProducer<P, T> name(String id) {
    return wrap(delegate.name(id));
  }


  @Override
  public <U> JProducer<P, U> merge(JProducer<P, U> r) {
    return wrap(delegate.merge(r.unwrap()));
  }

  @Override
  public JProducer<P, T> filter(Function1<T, Boolean> f) {
    return wrap(delegate.filter(eraseReturnType(f)));
  }

  @Override
  public <V> JKeyedProducer<P, T, Option<V>> lookup(Service<P, ?, T, V> service) {
    return wrap(delegate.<T, V>lookup(service.unwrap()));
  }

  @Override
  public <U> JProducer<P, U> map(Function1<T, U> f) {
    return wrap(delegate.map(f));
  }

  @Override
  public <U> JProducer<P, U> optionMap(Function1<T, Option<U>> f) {
    return wrap(delegate.optionMap(f));
  }

  @Override
  public <U> JProducer<P, U> flatMap(Function1<T, Iterable<U>> f) {
    return wrap(delegate.flatMap(toTraversableOnce(f)));
  }

  @Override
  public <U> JTailProducer<P, T> write(Sink<P, ?, U> sink) {
    return wrap(delegate.write(sink.unwrap()));
  }

  @Override
  public <U> JProducer<P, Either<T, U>> either(JProducer<P, U> other) {
    return wrap(delegate.either(other.unwrap()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> JKeyedProducer<P, K, V> asKeyed() {
    // this is an unchecked cast
    // we will know it's not a Tuple2<K, V> only when we execute it
    return wrap(Producer$.MODULE$.toKeyed((Producer<P, Tuple2<K, V>>)delegate));
  }
}
