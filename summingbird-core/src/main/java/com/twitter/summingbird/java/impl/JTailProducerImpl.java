package com.twitter.summingbird.java.impl;

import com.twitter.summingbird.Platform;
import com.twitter.summingbird.TailProducer;
import com.twitter.summingbird.java.JProducer;
import com.twitter.summingbird.java.JTailProducer;

public class JTailProducerImpl<P extends Platform<P>, T> extends JProducerImpl<P, T> /*implements JTail<P>*/ implements JTailProducer<P, T> {

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
    return wrap(delegate.also(that.unwrap(), null)); // DummyImplicit ???
  }

  @Override
  public <R> JProducer<P, R> also(JProducer<P, R> that) {
    return wrap(delegate.also(that.unwrap()));
  }

  @Override
  public JTailProducer<P, T> name(String id) {
    return wrap(delegate.name(id));
  }
}
