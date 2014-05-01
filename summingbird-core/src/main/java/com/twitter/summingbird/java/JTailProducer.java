package com.twitter.summingbird.java;

import com.twitter.summingbird.Platform;
import com.twitter.summingbird.TailProducer;

public interface JTailProducer<P extends Platform<P>, T> extends JProducer<P, T> {

  TailProducer<P, T> unwrap();

  <R> JTailProducer<P, R> also(JTailProducer<P, R> that);

  <R> JProducer<P, R> also(JProducer<P, R> that);

}