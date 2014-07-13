package com.twitter.summingbird.javaapi.impl;

import scala.Option;
import scala.Tuple2;

import com.twitter.summingbird.Platform;
import com.twitter.summingbird.Summer;
import com.twitter.summingbird.javaapi.JProducer;
import com.twitter.summingbird.javaapi.JSummer;
import com.twitter.summingbird.javaapi.JTailProducer;

public class JSummerImpl<P extends Platform<P>, K, V>
  extends JKeyedProducerImpl<P, K, Tuple2<Option<V>, V>>
  implements JSummer<P, K, V> {

  private final Summer<P, K, V> delegate;

  JSummerImpl(Summer<P, K, V> delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  @Override
  public Summer<P, K, V> unwrap() {
    return delegate;
  }

  @Override
  public <R> JTailProducer<P, R> also(JTailProducer<P, R> that) {
    return JTailProducerImpl.also(this, that);
  }

  @Override
  public <R> JProducer<P, R> also(JProducer<P, R> that) {
    return JTailProducerImpl.also(this, that);
  }

  @Override
  public JTailProducer<P, Tuple2<K, Tuple2<Option<V>, V>>> name(String id) {
    return JTailProducerImpl.name(this, id);
  }

}
