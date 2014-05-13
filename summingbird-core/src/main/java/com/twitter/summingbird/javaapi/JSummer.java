package com.twitter.summingbird.javaapi;

import scala.Option;
import scala.Tuple2;

import com.twitter.summingbird.Platform;
import com.twitter.summingbird.Summer;

/**
 * Wraps a Summer (a KeyedProducer that is also a TailProducer)
 *
 * @author Julien Le Dem
 *
 * @param <P> underlying Platform
 * @param <K> key type
 * @param <V> value type
 */
public interface JSummer<P extends Platform<P>, K, V>
  extends
    JKeyedProducer<P, K, Tuple2<Option<V>, V>>,
    JTailProducer<P, Tuple2<K, Tuple2<Option<V>, V>>> {

  /**
   * @return the wrapped Summer
   */
  Summer<P, K, V> unwrap();

}
