/*
 Copyright 2013 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.online._
import com.twitter.util.Future
import java.util.{ Collections, HashMap, Map => JMap, UUID }
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.SynchronizedMap
import java.util.WeakHashMap
import scala.collection.JavaConverters._

object TestStore {
  private val testStores = new WeakHashMap[String, TestStore[_, _]]

  def apply[K, V: Semigroup](storeID: String): Option[TestStore[K, V]] =
    (Option(testStores.get(storeID)).map { s =>
      s.asInstanceOf[TestStore[K, V]]
    })

  private def buildStore[K, V: Semigroup](initialData: Map[K, V]): String = {
    val storeID = UUID.randomUUID.toString
    val newInitStore = TestStore[K, V](storeID, initialData)
    testStores.synchronized {
      testStores.put(storeID, newInitStore)
    }
    storeID
  }

  def createBatchedStore[K, V](initialData: Map[(K, BatchID), V] = Map.empty[(K, BatchID), V])(implicit batcher: Batcher, valueSG: Semigroup[V]): (String, MergeableStoreFactory[(K, BatchID), V]) = {

    val storeID = buildStore[(K, BatchID), V](initialData)
    val supplier = MergeableStoreFactory.from(
      TestStore.apply[(K, BatchID), V](storeID)
        .getOrElse(sys.error("Weak hash map no longer contains store"))
    )
    (storeID, supplier)
  }

  def createStore[K, V: Semigroup](initialData: Map[K, V] = Map.empty[K, V]): (String, MergeableStoreFactory[(K, BatchID), V]) = {
    val storeID = buildStore[K, V](initialData)
    val supplier = MergeableStoreFactory.fromOnlineOnly(
      TestStore.apply[K, V](storeID)
        .getOrElse(sys.error("Weak hash map no longer contains store"))
    )

    (storeID, supplier)
  }
}

case class TestStore[K, V: Semigroup](storeID: String, initialData: Map[K, V]) extends MergeableStore[K, V] {
  private val backingStore: JMap[K, Option[V]] =
    Collections.synchronizedMap(new HashMap[K, Option[V]]())
  val updates: AtomicInteger = new AtomicInteger(0)
  val reads: AtomicInteger = new AtomicInteger(0)

  def toScala: Map[K, V] = backingStore.asScala.collect { case (k, Some(v)) => (k, v) }.toMap

  private def getOpt(k: K) = {
    reads.incrementAndGet
    Option(backingStore.get(k)).flatMap(i => i)
  }

  val semigroup = implicitly[Semigroup[V]]

  override def get(k: K) = Future.value(getOpt(k))

  override def put(pair: (K, Option[V])) = {
    val (k, optV) = pair
    if (optV.isDefined)
      backingStore.put(k, optV)
    else
      backingStore.remove(k)
    updates.incrementAndGet
    Future.Unit
  }

  override def merge(pair: (K, V)) = {
    val (k, v) = pair
    val oldV = getOpt(k)
    val newV = Semigroup.plus(Some(v), oldV)
    updates.incrementAndGet
    backingStore.put(k, newV)
    Future.value(oldV)
  }

}
