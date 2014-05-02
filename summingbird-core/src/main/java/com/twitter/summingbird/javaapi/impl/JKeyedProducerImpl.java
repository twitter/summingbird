package com.twitter.summingbird.javaapi.impl;

import scala.Function1;
import scala.Option;
import scala.Tuple2;

import com.twitter.algebird.Semigroup;
import com.twitter.summingbird.KeyedProducer;
import com.twitter.summingbird.Platform;
import com.twitter.summingbird.javaapi.Buffer;
import com.twitter.summingbird.javaapi.JKeyedProducer;
import com.twitter.summingbird.javaapi.JProducer;
import com.twitter.summingbird.javaapi.JSummer;
import com.twitter.summingbird.javaapi.Service;
import com.twitter.summingbird.javaapi.Store;

public class JKeyedProducerImpl<P extends Platform<P>, K, V> extends JProducerImpl<P, Tuple2<K, V>> implements JKeyedProducer<P, K, V> {

  private KeyedProducer<P, K, V> delegate;

  JKeyedProducerImpl(KeyedProducer<P, K, V> delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  @Override
  public KeyedProducer<P, K, V> unwrap() {
    return delegate;
  }

  @Override
  public JKeyedProducer<P, K, V> filterKeys(Function1<K, Boolean> f) {
    return wrap(delegate.filterKeys(eraseReturnType(f)));
  }

  @Override
  public JKeyedProducer<P, K, V> filterValues(Function1<V, Boolean> f) {
    return wrap(delegate.filterValues(eraseReturnType(f)));
  }

  @Override
  public <K2> JKeyedProducer<P, K2, V> flatMapKeys(Function1<K, Iterable<K2>> f) {
    return wrap(delegate.flatMapKeys(toTraversableOnce(f)));
  }

  @Override
  public <V2> JKeyedProducer<P, K, V2> flatMapValues(Function1<V, Iterable<V2>> f) {
    return wrap(delegate.flatMapValues(toTraversableOnce(f)));
  }

  @Override
  public JProducer<P, K> keys() {
    return wrap(delegate.keys());
  }

  @Override
  public <RightV> JKeyedProducer<P, K, Tuple2<V, Option<RightV>>> leftJoin(Service<P, ?, K, RightV> service) {
    return wrap(delegate.<RightV>leftJoin(service.unwrap()));
  }

  @Override
  public <RightV> JKeyedProducer<P, K, Tuple2<V, Option<RightV>>> leftJoin(JKeyedProducer<P, K, RightV> stream, Buffer<P, ?, K, RightV> buffer) {
    return wrap(delegate.<RightV>leftJoin(stream.unwrap(), buffer.unwrap()));
  }

  @Override
  public <K2> JKeyedProducer<P, K2, V> mapKeys(Function1<K, K2> f) {
    return wrap(delegate.mapKeys(f));
  }

  @Override
  public <V2> JKeyedProducer<P, K, V2> mapValues(Function1<V, V2> f) {
    return wrap(delegate.mapValues(f));
  }

  @Override
  public JSummer<P, K, V> sumByKey(Store<P, ?, K, V> store, Semigroup<V> semigroup) {
    return wrap(delegate.sumByKey(store.unwrap(), semigroup));
  }

  @Override
  public JKeyedProducer<P, V, K> swap() {
    return wrap(delegate.swap());
  }

  @Override
  public JProducer<P, V> values() {
    return wrap(delegate.values());
  }

}
