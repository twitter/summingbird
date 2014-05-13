package com.twitter.summingbird.javaapi;

import com.twitter.summingbird.Platform;

/**
 * a Store used in sumByKey
 *
 * @author Julien Le Dem
 *
 * @param <P> the underlying platform
 * @param <STORE> Is the actual type used by the underlying Platform it is parameterized in <K,V>
 * @param <K> key
 * @param <V> value
 */
public class Store<P extends Platform<P>, STORE, K, V> extends Wrapper<STORE> {

  public Store(STORE store) {
    super(store);
  }

}
