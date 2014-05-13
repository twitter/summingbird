package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;

/**
 * a Buffer used in left joins
 *
 * @author Julien Le Dem
 *
 * @param <P> the underlying platform
 * @param <BUFFER> Is the actual type used by the underlying Platform it is parameterized in <K,V>
 * @param <K> key
 * @param <V> value
 */
public class Buffer<P extends Platform<P>, BUFFER, K, V> extends Wrapper<BUFFER> {

  public Buffer(BUFFER buffer) {
    super(buffer);
  }

}
