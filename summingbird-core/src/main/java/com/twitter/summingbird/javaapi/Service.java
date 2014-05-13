package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;

/**
 * a Service used in lookups
 *
 * @author Julien Le Dem
 *
 * @param <P> the underlying platform
 * @param <SERVICE> Is the actual type used by the underlying Platform it is parameterized in <K,V>
 * @param <K> key
 * @param <V> value
 */
public class Service<P extends Platform<P>, SERVICE, K, V> extends Wrapper<SERVICE> {

  public Service(SERVICE service) {
    super(service);
  }

}
