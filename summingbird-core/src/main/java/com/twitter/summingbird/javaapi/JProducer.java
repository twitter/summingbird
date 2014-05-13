package com.twitter.summingbird.javaapi;

import scala.Option;
import scala.Tuple2;

import com.twitter.summingbird.Platform;
import com.twitter.summingbird.Producer;

/**
 * Wraps a Producer
 *
 * @author Julien Le Dem
 *
 * @param <P> underlying Platform
 * @param <T> record type
 */
public interface JProducer<P extends Platform<P>, T> {

  /**
   * @return the wrapped Producer
   */
  Producer<P, T> unwrap();

  /**
   * names this producer
   * @param id the name of the producer
   * @return
   */
  JProducer<P, T> name(String id);

  // merge, lookup and write are invariant unlike the scala api because java does not allow lower bounds in type parameters

  /**
   * merges two producers of the same type
   * @param r
   * @return
   */
  JProducer<P, T> merge(JProducer<P, T> r);

  // collect does not really apply in java < 8

  /**
   * filters this by applying the provided predicate
   * @param p
   * @return
   */
  JProducer<P, T> filter(Predicate<T> p);

  /**
   * looks up the values in the provided service and returns key-value tuples of the optional result
   * @param service
   * @return
   */
  <V> JKeyedProducer<P, T, Option<V>> lookup(Service<P, ?, T, V> service);

  /**
   * applies the function to the values
   * @param f
   * @return
   */
  <U> JProducer<P, U> map(Function<T, U> f);

  /**
   * applies the function to the values and flattens the result
   * (preferred to flatMap when possible to enable optimizations)
   * @param f
   * @return
   */
  <U> JProducer<P, U> optionMap(Function<T, Option<U>> f);

  /**
   * applies the function to the values and flattens the result
   * @param f
   * @return
   */
  <U> JProducer<P, U> flatMap(Function<T, ? extends Iterable<U>> f);

  /**
   * writes the result to the provided Sink
   * @param sink
   * @return
   */
  JTailProducer<P, T> write(Sink<P, ?, T> sink);

  // Either does not cross compile between scala 2.9 and 2.10 as it moved from scala to scala.util
//  <U> JProducer<P, Either<T, U>> either(JProducer<P, U> other);

  /**
   * turns a Producer into a KeyedProducer by applying the provided function returning key-value tuples
   * @param f
   * @return
   */
  <K, V> JKeyedProducer<P, K, V> mapToKeyed(Function<T, Tuple2<K, V>> f);

}