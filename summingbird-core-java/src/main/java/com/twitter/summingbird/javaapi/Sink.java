package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;

/**
 * a Sink used to write to
 *
 * @author Julien Le Dem
 *
 * @param <P> the underlying platform
 * @param <SINK> Is the actual type used by the underlying Platform it is parameterized in <T>
 * @param <T> written type
 */
public class Sink<P extends Platform<P>, SINK, T> extends Wrapper<SINK> {

  public Sink(SINK sink) {
    super(sink);
  }
}
