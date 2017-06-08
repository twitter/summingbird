package com.twitter.summingbird

import com.twitter.algebird.Semigroup
import org.scalacheck.Arbitrary

class TestPlatform extends Platform[TestPlatform] {
  override type Source[T] = TestPlatform.SourceList[T]
  override type Store[K, V] = TestPlatform.Store[K, V]
  override type Sink[T] = TestPlatform.Sink[T]
  override type Service[K, V] = TestPlatform.Service[K, V]
  override type Plan[T] = TailProducer[TestPlatform, T]

  override def plan[T](completed: TailProducer[TestPlatform, T]): TailProducer[TestPlatform, T] = completed
}

object TestPlatform {
  // Test `Source` should accept data with time, not just plain data.
  case class SourceList[T](data: List[T])
  case class Store[K, V: Semigroup](id: String, initial: Map[K, V]) {
    val valueSemigroup: Semigroup[V] = implicitly[Semigroup[V]]
  }
  case class Sink[T](id: String)
  case class Service[K, V](fn: K => Option[V])

  def source[T](data: List[T]): Source[TestPlatform, T] = Source[TestPlatform, T](SourceList(data))

  /**
   * Returns `TestPlatform#Store` with initially random content.
   */
  def store[K: Arbitrary, V: Arbitrary: Semigroup](id: String): Store[K, V] = {
    Store(id, implicitly[Arbitrary[Map[K, V]]].arbitrary.sample.get)
  }

  /**
   * For consistency with source and store.
   */
  def service[K, V](fn: K => Option[V]): Service[K, V] = Service(fn)
  def sink[T](id: String): Sink[T] = Sink(id)
}
