package com.twitter.summingbird.store

import org.scalatest.WordSpec

import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.batch._
import com.twitter.util.{ Await, Future }

/**
 * The backing map of a TestStore holds an Option[V] -- keys that are
 * present in the map will be mapped to successful futures. Missing
 * keys are interpreted as failed futures.
 */

case class TestStore[K, +V](m: Map[K, Option[V]]) extends ReadableStore[K, V] {
  override def get(k: K) =
    m.get(k).map { Future.value(_) }
      .getOrElse(Future.exception(new RuntimeException("fail!")))
}

class ClientStoreLaws extends WordSpec {
  /** Batcher that always returns a batch of 10. */
  implicit val batcher = new AbstractBatcher {
    def batchOf(t: Timestamp) = BatchID(10)
    def earliestTimeOf(batch: BatchID) = Timestamp(0)
  }

  val offline = TestStore[String, (BatchID, Int)](
    Map(
      "a" -> Some((BatchID(8), 1)),
      "b" -> Some((BatchID(9), 1)),
      "c" -> Some((BatchID(9), 1)),
      "d" -> None,
      "e" -> None
    ))

  val online = TestStore[(String, BatchID), Int](
    Map(
      ("a", BatchID(6)) -> Some(1), ("a", BatchID(7)) -> Some(1), ("a", BatchID(8)) -> Some(1), ("a", BatchID(9)) -> None, ("a", BatchID(10)) -> Some(1),
      ("b", BatchID(9)) -> None, ("b", BatchID(10)) -> None,
      ("c", BatchID(6)) -> Some(1), ("c", BatchID(7)) -> Some(1), ("c", BatchID(8)) -> Some(1), ("c", BatchID(9)) -> None, ("c", BatchID(10)) -> Some(1),
      ("d", BatchID(8)) -> None, ("d", BatchID(9)) -> None, ("d", BatchID(10)) -> None,
      ("f", BatchID(8)) -> Some(2), ("f", BatchID(9)) -> Some(3), ("f", BatchID(10)) -> Some(4)
    )
  )
  val clientStore = ClientStore(offline, online, 3)
  val keys = Set("a", "b", "c", "d", "e", "f")
  val retMap = clientStore.multiGet(keys)

  def assertPresent[T](f: Future[T], comparison: T) {
    assert(Await.result(f.liftToTry).isReturn && Await.result(f) == comparison)
  }

  "ClientStore should return a map from multiGet of the same size as the input request" in {
    assert(keys == retMap.keySet)
  }
  "ClientStore should return Some(v) (properly merged) if either offline and online have values" in {
    assertPresent(retMap("a"), Some(3))
    assertPresent(retMap("b"), Some(1))
    assertPresent(retMap("c"), Some(2))
  }
  "ClientStore should return None if both offline and online have None for all batches" in {
    assertPresent(retMap("d"), None)
  }
  "ClientStore should fail a key when offline succeeds and online fails" in {
    assert(Await.result(retMap("e").liftToTry).isThrow)
  }
  "ClientStore should fail a key when offline fails and online succeeds" in {
    assert(Await.result(retMap("f").liftToTry).isThrow)
  }
}
