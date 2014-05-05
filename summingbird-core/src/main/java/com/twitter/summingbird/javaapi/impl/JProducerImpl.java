package com.twitter.summingbird.javaapi.impl;

import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.TraversableOnce;
import scala.runtime.AbstractFunction1;
import scala.util.Either;

import com.twitter.summingbird.KeyedProducer;
import com.twitter.summingbird.Platform;
import com.twitter.summingbird.Producer;
import com.twitter.summingbird.Producer$;
import com.twitter.summingbird.Summer;
import com.twitter.summingbird.TailProducer;
import com.twitter.summingbird.javaapi.Function;
import com.twitter.summingbird.javaapi.JKeyedProducer;
import com.twitter.summingbird.javaapi.JProducer;
import com.twitter.summingbird.javaapi.JSummer;
import com.twitter.summingbird.javaapi.JTailProducer;
import com.twitter.summingbird.javaapi.Predicate;
import com.twitter.summingbird.javaapi.Service;
import com.twitter.summingbird.javaapi.Sink;
import com.twitter.summingbird.javaapi.Source;

public class JProducerImpl<P extends Platform<P>, T> implements JProducer<P, T> {

  public static <P extends Platform<P>, T> JProducer<P, T> source(Source<P, ?, T> source) {
    return new JProducerImpl<P, T>(Producer$.MODULE$.<P, T>source(source.unwrap()));
  }

  static <IN, OUT> Function1<IN, TraversableOnce<OUT>> toTraversableOnce(final Function<IN, Iterable<OUT>> f) {
    return new AbstractFunction1<IN, TraversableOnce<OUT>>() {
      public TraversableOnce<OUT> apply(IN v) {
        return JavaConversions.iterableAsScalaIterable(f.apply(v));
      }
    };
  }

  static <IN, OUT> Function1<IN, OUT> toScala(final Function<IN, OUT> f) {
    return new AbstractFunction1<IN, OUT>() {
      @Override
      public OUT apply(IN v) {
        return f.apply(v);
      }
    };
  }

  static <IN> Function1<IN, Object> toScala(final Predicate<IN> p) {
    return new AbstractFunction1<IN, Object>() {
      @Override
      public Object apply(IN v) {
        return p.test(v);
      }
    };
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
  public JProducer<P, T> merge(JProducer<P, T> r) {
    return wrap(delegate.merge(r.unwrap()));
  }

  @Override
  public JProducer<P, T> filter(Predicate<T> f) {
    return wrap(delegate.filter(toScala(f)));
  }

  @Override
  public <V> JKeyedProducer<P, T, Option<V>> lookup(Service<P, ?, T, V> service) {
    return wrap(delegate.<T, V>lookup(service.unwrap()));
  }

  @Override
  public <U> JProducer<P, U> map(Function<T, U> f) {
    return wrap(delegate.map(toScala(f)));
  }

  @Override
  public <U> JProducer<P, U> optionMap(Function<T, Option<U>> f) {
    return wrap(delegate.optionMap(toScala(f)));
  }

  @Override
  public <U> JProducer<P, U> flatMap(Function<T, Iterable<U>> f) {
    return wrap(delegate.flatMap(toTraversableOnce(f)));
  }

  @Override
  public JTailProducer<P, T> write(Sink<P, ?, T> sink) {
    return wrap(delegate.write(sink.unwrap()));
  }

  @Override
  public <U> JProducer<P, Either<T, U>> either(JProducer<P, U> other) {
    return wrap(delegate.either(other.unwrap()));
  }

  @Override
  public <K, V> JKeyedProducer<P, K, V> mapToKeyed(Function<T, Tuple2<K, V>> f) {
    return new JKeyedProducerImpl<P, K, V>(map(f));
  }
}
