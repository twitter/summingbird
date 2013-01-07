package com.twitter.summingbird.store

import com.twitter.util.Future
import com.twitter.summingbird.util.FutureUtil

import FutureUtil._

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// Store is immutable by default.

trait Store[Self <: Store[Self,K,V],K,V] {
  def get(k: K): Future[Option[V]] = single(k) { multiGet(_) }
  def multiGet(ks: Set[K]): Future[Map[K,V]] = collectValues(ks) { get(_) }
  def -(k: K): Future[Self]
  def +(pair: (K,V)): Future[Self]
  def update(k: K)(fn: Option[V] => Option[V]): Future[Self] = {
    get(k) flatMap { opt: Option[V] =>
      fn(opt)
        .map { v => this + (k -> v) }
        .getOrElse(this - k)
    }
  }
}

trait KeysetStore[Self <: KeysetStore[Self,K,V],K,V] extends Store[Self,K,V] {
  def keySet: Set[K]
  def size : Int
}

// Used as a constraint annotation.
trait MutableStore[Self <: MutableStore[Self,K,V],K,V] extends Store[Self,K,V]

// Used as a constraint annotation.
trait ConcurrentMutableStore[Self <: ConcurrentMutableStore[Self,K,V],K,V] extends MutableStore[Self,K,V]
