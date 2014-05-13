package com.twitter.summingbird.javaapi;

import scala.Option;
import scala.Some;
import scala.Tuple2;

import com.twitter.algebird.Semigroup;
import com.twitter.algebird.Semigroup$;
import com.twitter.summingbird.Platform;
import com.twitter.summingbird.javaapi.impl.JKeyedProducerImpl;
import com.twitter.summingbird.javaapi.impl.JProducerImpl;


/**
 * Helpers to help interfacing with scala
 *
 * @author Julien Le Dem
 *
 */
public class JProducers {

  /**
   * Wraps a source to pass to get a JProducer
   * @param source
   * @return
   */
  public static <P extends Platform<P>, T> JProducer<P, T> source(Source<P, ?, T> source) {
    return JProducerImpl.source(source);
  }

  /**
   * converts a JProducer to a JKeyedProducer when T is actually a Tuple2<K, V>
   * @param producer
   * @return
   */
  public static <P extends Platform<P>, T, K, V> JKeyedProducer<P, K, V> toKeyed(JProducer<P, Tuple2<K, V>> producer) {
    return JKeyedProducerImpl.toKeyed(producer);
  }

  /**
   * @return None<T>
   */
  public static <T> Option<T> none() {
    return scala.Option$.MODULE$.<T>empty();
  }

  /**
   * @param t
   * @return Some<T>(t)
   */
  public static <T> Option<T> some(T t) {
    if (t == null) {
      throw new NullPointerException("some(null)");
    }
    return new Some<T>(t);
  }

  /**
   * @param t
   * @return Option<T>(t)
   */
  public static <T> Option<T> option(T t) {
    return scala.Option$.MODULE$.apply(t);
  }

}
