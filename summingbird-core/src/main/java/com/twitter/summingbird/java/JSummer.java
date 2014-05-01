package com.twitter.summingbird.java;

import scala.Option;
import scala.Tuple2;

import com.twitter.summingbird.Platform;
import com.twitter.summingbird.Summer;

public interface JSummer<P extends Platform<P>, K, V>
  extends
    JKeyedProducer<P, K, Tuple2<Option<V>, V>>,
    JTailProducer<P, Tuple2<K, Tuple2<Option<V>, V>>> {

  Summer<P, K, V> unwrap();

}
