package com.twitter.summingbird.store

import java.util.{ LinkedHashMap => JLinkedHashMap, Map => JMap }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object LRUStore {
  def apply[K,V](maxSize: Int = 1000) = new LRUStore[K,V](maxSize)
}
class LRUStore[K,V](maxSize: Int) extends JMapStore[LRUStore[K,V],K,V] {
  // create a java linked hashmap with access-ordering (LRU)
  protected lazy override val jstore = new JLinkedHashMap[K,Option[V]](maxSize + 1, 0.75f, true) {
    override protected def removeEldestEntry(eldest : JMap.Entry[K, Option[V]]) =
      super.size > maxSize
  }
}
