package com.twitter.summingbird.store

import com.twitter.util.Future
import com.twitter.summingbird.util.FutureUtil.zipWith

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// MapStore is an immutable store.
class MapStore[K,V](val backingStore: Map[K,V] = Map[K,V]()) extends KeysetStore[MapStore[K,V],K,V] {
  override def keySet = backingStore.keySet
  override def size = backingStore.size
  override def get(k: K) = Future.value(backingStore.get(k))
  override def multiGet(ks: Set[K]) = Future.value(zipWith(ks) { backingStore.get(_) })
  override def -(k: K) = Future.value(new MapStore(backingStore - k))
  override def +(pair: (K, V)) = Future.value(new MapStore(backingStore + pair))
}
