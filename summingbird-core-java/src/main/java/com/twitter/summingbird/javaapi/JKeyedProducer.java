package com.twitter.summingbird.javaapi;

import scala.Option;
import scala.Tuple2;

import com.twitter.algebird.Semigroup;
import com.twitter.summingbird.KeyedProducer;
import com.twitter.summingbird.Platform;

/**
 * Wraps a KeyedProducer (operations on key-value tuples)
 *
 * @author Julien Le Dem
 *
 * @param <P> underlying Platform
 * @param <K> key type
 * @param <V> value type
 */
public interface JKeyedProducer<P extends Platform<P>, K, V> extends JProducer<P, Tuple2<K, V>> {

  /**
   * @return the wrapped KeyedProducer
   */
  KeyedProducer<P, K, V> unwrap();

  // collectKeys and collectValues not useful in java as there is no PartialFunction

  /**
   * filters the producer by keeping the key-value tuples whose keys satisfy the predicate
   * @param p predicate
   * @return the filtered producer
   */
  JKeyedProducer<P, K, V> filterKeys(Predicate<K> p);

  /**
   * filters the producer by keeping the key value tuples whose values satisfy the predicate
   * @param p predicate
   * @return the filtered producer
   */
  JKeyedProducer<P, K, V> filterValues(Predicate<V> p);

  /**
   * flattens the result of f on keys into a new Producer
   * @param f
   * @return the resulting producer
   */
  <K2> JKeyedProducer<P, K2, V> flatMapKeys(Function<K, ? extends Iterable<K2>> f);

  /**
   * flattens the result of f on values into a new Producer
   * @param f
   * @return the resulting producer
   */
  <V2> JKeyedProducer<P, K, V2> flatMapValues(Function<V, ? extends Iterable<V2>> f);

  /**
   * @return a producer with only the keys
   */
  JProducer<P, K> keys();

  /**
   * looks up the keys in the provided service and adds the Optional result to the tuple
   * @param service
   * @return the resulting Producer
   */
  <RightV> JKeyedProducer<P, K, Tuple2<V, Option<RightV>>> leftJoin(Service<P, ?, K, RightV> service);

  /**
   * Joins the two keyed producers using the provided Buffer
   * @param stream the right stream to join this with
   * @param buffer the buffer used to store stream for joining with this.
   * @return the resulting Producer
   */
  <RightV> JKeyedProducer<P, K, Tuple2<V, Option<RightV>>> leftJoin(JKeyedProducer<P, K, RightV> stream, Buffer<P, ?, K, RightV> buffer);

  /**
   * applies f to keys
   * @param f
   * @return
   */
  <K2> JKeyedProducer<P, K2, V> mapKeys(Function<K, K2> f);

  /**
   * applies f to values
   * @param f
   * @return
   */
  <V2> JKeyedProducer<P, K, V2> mapValues(Function<V, V2> f);

  /**
   * sums by key using the provided store to persist the result
   * @param store
   * @param semigroup the definition of the Sum to apply
   * @return
   */
  JSummer<P, K, V> sumByKey(Store<P, ?, K, V> store, Semigroup<V> semigroup);

  /**
   * swaps key and value
   * @return
   */
  JKeyedProducer<P, V, K> swap();

  /**
   * @return a producer with only the values
   */
  JProducer<P, V> values();
}