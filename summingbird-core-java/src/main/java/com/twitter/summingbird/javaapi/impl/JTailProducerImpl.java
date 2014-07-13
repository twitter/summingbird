package com.twitter.summingbird.javaapi.impl;

import com.twitter.summingbird.Platform;
import com.twitter.summingbird.TailProducer;
import com.twitter.summingbird.javaapi.JProducer;
import com.twitter.summingbird.javaapi.JTailProducer;

public class JTailProducerImpl<P extends Platform<P>, T> extends JProducerImpl<P, T> implements JTailProducer<P, T> {

  static <P extends Platform<P>, T, R> JProducer<P, R> also(JTailProducer<P, T> tp1, JProducer<P, R> p2) {
    return wrap(tp1.unwrap().also(p2.unwrap()));
  }

  static <P extends Platform<P>, T, R> JTailProducer<P, R> also(JTailProducer<P, T> tp1, JTailProducer<P, R> tp2) {
    return wrap(tp1.unwrap().also(tp2.unwrap(), null));
    // second parameter of also is a DummyImplicit used to differentiate with the other also above
  }

  static <P extends Platform<P>, T> JTailProducer<P, T> name(JTailProducer<P, T> tp, String id) {
    return wrap(tp.unwrap().name(id));
  }

  private TailProducer<P, T> delegate;

  JTailProducerImpl(TailProducer<P, T> delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  @Override
  public TailProducer<P, T> unwrap() {
    return delegate;
  }

  @Override
  public <R> JTailProducer<P, R> also(JTailProducer<P, R> that) {
    return also(this, that);
  }

  @Override
  public <R> JProducer<P, R> also(JProducer<P, R> that) {
    return also(this, that);
  }

  @Override
  public JTailProducer<P, T> name(String id) {
    return name(this, id);
  }
}
