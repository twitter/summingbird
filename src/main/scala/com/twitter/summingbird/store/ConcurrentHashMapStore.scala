package com.twitter.summingbird.store

import java.util.concurrent.{ ConcurrentHashMap => JConcurrentHashMap }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

class ConcurrentHashMapStore[K,V]
extends JMapStore[ConcurrentHashMapStore[K,V],K,V]
with ConcurrentMutableStore[ConcurrentHashMapStore[K,V],K,V] {
  protected lazy override val jstore = new JConcurrentHashMap[K,Option[V]]
}
