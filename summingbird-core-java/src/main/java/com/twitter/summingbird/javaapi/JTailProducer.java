package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;
import com.twitter.summingbird.TailProducer;

/**
 * wraps a TailProducer (a Producer that can be planned)
 *
 * @author Julien Le Dem
 *
 * @param <P> the underlying platform
 * @param <T> the type of records
 */
public interface JTailProducer<P extends Platform<P>, T> extends JProducer<P, T> {

  /**
   * @return the wrapped TailProducer
   */
  TailProducer<P, T> unwrap();

  /**
   * chains that after this
   * @param that
   * @return
   */
  <R> JTailProducer<P, R> also(JTailProducer<P, R> that);

  /**
   * chains that after this
   * @param that
   * @return
   */
  <R> JProducer<P, R> also(JProducer<P, R> that);

}