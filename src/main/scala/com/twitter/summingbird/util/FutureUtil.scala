package com.twitter.summingbird.util

import com.twitter.util.Future

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

object FutureUtil {
  // TODO: Move to some collection util.
  def zipWith[K,V](keys: Set[K])(lookup: (K) => Option[V]): Map[K,V] =
    keys.foldLeft(Map.empty[K,V]) { (m,k) =>
      lookup(k) map { v => m + (k -> v) } getOrElse m
    }

  // TODO: Move to com.twitter.util.Future.
  def collectValues[K,V](keys: Set[K])
  (lookup: (K) => Future[Option[V]]): Future[Map[K,V]] = {
    val futures: Seq[Future[Option[(K,V)]]] =
      keys.toSeq map { k => lookup(k) map { _ map { (k,_) } } }
    Future.collect(futures) map { _.flatten.toMap }
  }
  def single[K,V](k: K)(multiGet: (Set[K]) => Future[Map[K,V]]): Future[Option[V]] =
    multiGet(Set(k)) map { _.get(k) }
}
