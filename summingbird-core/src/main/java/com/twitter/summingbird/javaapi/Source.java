package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;

/**
 * a Source used to read from
 *
 * @author Julien Le Dem
 *
 * @param <P> the underlying platform
 * @param <SOURCE> Is the actual type used by the underlying Platform it is parameterized in <T>
 * @param <T> written type
 */
public class Source<P extends Platform<P>, SOURCE, T> extends Wrapper<SOURCE> {

  public Source(SOURCE source) {
    super(source);
  }

}
