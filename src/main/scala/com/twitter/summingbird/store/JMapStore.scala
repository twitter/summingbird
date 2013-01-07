package com.twitter.summingbird.store

import com.twitter.util.Future
import com.twitter.summingbird.util.FutureUtil.zipWith

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

abstract class JMapStore[S <: JMapStore[S,K,V],K,V] extends MutableStore[S,K,V] {
  protected val jstore: java.util.Map[K,Option[V]]
  def storeGet(k: K): Option[V] = {
    val stored = jstore.get(k)
    if (stored != null)
      stored
    else
      None
  }
  override def get(k: K): Future[Option[V]] = Future.value(storeGet(k))
  override def multiGet(ks: Set[K]): Future[Map[K,V]] =
    Future.value(zipWith(ks) { storeGet(_) })

  override def -(k: K): Future[S] = {
    jstore.remove(k)
    Future.value(this.asInstanceOf[S])
  }
  override def +(pair: (K,V)): Future[S] = {
    jstore.put(pair._1, Some(pair._2))
    Future.value(this.asInstanceOf[S])
  }
}
