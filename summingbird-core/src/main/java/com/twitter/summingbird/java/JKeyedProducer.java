package com.twitter.summingbird.java;

import scala.Function1;
import scala.Option;
import scala.Tuple2;

import com.twitter.algebird.Semigroup;
import com.twitter.summingbird.KeyedProducer;
import com.twitter.summingbird.Platform;

public interface JKeyedProducer<P extends Platform<P>, K, V> extends JProducer<P, Tuple2<K, V>> {

  KeyedProducer<P, K, V> unwrap();

  // collectKeys and collectValues not useful in java ?

  JKeyedProducer<P, K, V> filterKeys(Function1<K, Boolean> f);

  JKeyedProducer<P, K, V> filterValues(Function1<V, Boolean> f);

  <K2> JKeyedProducer<P, K2, V> flatMapKeys(Function1<K, Iterable<K2>> f);

  <V2> JKeyedProducer<P, K, V2> flatMapValues(Function1<V, Iterable<V2>> f);

  JProducer<P, K> keys();

  <RightV> JKeyedProducer<P, K, Tuple2<V, Option<RightV>>> leftJoin(Service<P, ?, K, RightV> service);

  <RightV> JKeyedProducer<P, K, Tuple2<V, Option<RightV>>> leftJoin(JKeyedProducer<P, K, RightV> stream, Buffer<P, ?, K, RightV> buffer);

  <K2> JKeyedProducer<P, K2, V> mapKeys(Function1<K, K2> f);

  <V2> JKeyedProducer<P, K, V2> mapValues(Function1<V, V2> f);

  JSummer<P, K, V> sumByKey(Store<P, ?, K, V> store, Semigroup<V> semigroup);

  JKeyedProducer<P, V, K> swap();

  JProducer<P, V> values();
}