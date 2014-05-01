package com.twitter.summingbird.java.impl;

import scala.Option;
import scala.Tuple2;

import com.twitter.summingbird.Platform;
import com.twitter.summingbird.Summer;
import com.twitter.summingbird.java.JProducer;
import com.twitter.summingbird.java.JSummer;
import com.twitter.summingbird.java.JTailProducer;

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
    return wrap(delegate.also(that.unwrap(), null)); // DummyImplicit ???
  }

  @Override
  public <R> JProducer<P, R> also(JProducer<P, R> that) {
    return wrap(delegate.also(that.unwrap()));
  }

  @Override
  public JTailProducer<P, Tuple2<K, Tuple2<Option<V>, V>>> name(String id) {
    return wrap(delegate.name(id));
  }

}
