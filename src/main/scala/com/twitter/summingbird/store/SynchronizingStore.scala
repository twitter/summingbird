package com.twitter.summingbird.store

import com.twitter.util.Future

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * The SynchronizingStore allows the user to pair a temporary immutable
 * KeySetStore with some mutable backing store for longer-term persistence.
 *
 * At regular intervals, all key-value pairs in the in-memory localStore
 * will be snapshotted into the mutable remoteStore. This is useful when one
 * wants to serve key-value pairs out of both Storm bolts and some other random-write
 * persistence, like memcached. See com.twitter.summingbird.sink.CommittingSink
 * for a sink that flushes on every write.
 */

object SynchronizingStore {
  def apply[K,V](localStore: KeysetStore[_,K,V],
                 remoteStore: MutableStore[_ <: MutableStore[_, K, V],K,V],
                 syncIntervalInMillis: Long) = {
    val synchronizer = new StoreSynchronizer[K,V](syncIntervalInMillis, remoteStore)
    // Send the initial KeysetStore
    synchronizer ! localStore
    new SynchronizingStore[K,V](localStore, synchronizer)
  }
}

// TODO: the StoreSynchronizer is really just an observer on +/-. Register the
// synchronizer as an observer on those functions vs explicitly
// acknowledging it in these impls.
//
// The StoreSynchronizer has a closure over the mutable remoteStore referenced above.
class SynchronizingStore[K,V](localStore: KeysetStore[_,K,V], synchronizer: StoreSynchronizer[K,V])
extends KeysetStore[SynchronizingStore[K,V],K,V] {

  def start { synchronizer.start }
  def stop { synchronizer ! 'stop }

  override def size = localStore.size
  override def keySet = localStore.keySet
  override def get(k: K) = localStore.get(k)
  override def multiGet(ks: Set[K]) = localStore.multiGet(ks)
  override def -(k: K) = {
    val next = (localStore - k).asInstanceOf[KeysetStore[_,K,V]]
    synchronizer ! next
    Future.value(new SynchronizingStore[K,V](next, synchronizer))
  }
  override def +(pair: (K,V)) = {
    val next = (localStore + pair).asInstanceOf[KeysetStore[_,K,V]]
    synchronizer ! next
    Future.value(new SynchronizingStore[K,V](next, synchronizer))
  }
}
