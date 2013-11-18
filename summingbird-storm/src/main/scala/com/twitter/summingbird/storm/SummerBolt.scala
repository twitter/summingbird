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

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.tuple.{Tuple, Values, Fields}
import com.twitter.algebird.{Semigroup, SummingQueue}
import com.twitter.summingbird.online.Externalizer
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.summingbird.batch.{BatchID, Timestamp}
import com.twitter.summingbird.storm.option._
import com.twitter.summingbird.option.CacheSize

import com.twitter.util.{Await, Future}
import java.util.{ Arrays => JArrays, List => JList, Map => JMap, ArrayList => JAList }

/**
  * The SummerBolt takes two related options: CacheSize and MaxWaitingFutures.
  * CacheSize sets the number of key-value pairs that the SinkBolt will accept
  * (and sum into an internal map) before committing out to the online store.
  *
  * To perform this commit, the SinkBolt iterates through the map of aggregated
  * kv pairs and performs a "+" on the store for each pair, sequencing these
  * "+" calls together using the Future monad. If the store has high latency,
  * these calls might take a bit of time to complete.
  *
  * MaxWaitingFutures(count) handles this problem by realizing a future
  * representing the "+" of kv-pair n only when kvpair n + 100 shows up in the bolt,
  * effectively pushing back against latency bumps in the host.
  *
  * The allowed latency before a future is forced is equal to
  * (MaxWaitingFutures * execute latency).
  *
  * @author Oscar Boykin
  * @author Sam Ritchie
  * @author Ashu Singhal
  */

object JListSemigroup {
  def lift[T](t: T): JAList[T] = { val l = new JAList[T](); l.add(t); l }

  implicit def jlistConcat[T]: Semigroup[JAList[T]] = new JListSemigroup[T]
}
/** Mutably concat java lists */
class JListSemigroup[T] extends Semigroup[JAList[T]] {
  def plus(old: JAList[T], next: JAList[T]): JAList[T] = {
    old.addAll(next)
    old
  }
}

class SummerBolt[Key, Value: Semigroup](
  @transient storeSupplier: () => MergeableStore[(Key,BatchID), Value],
  @transient successHandler: OnlineSuccessHandler,
  @transient exceptionHandler: OnlineExceptionHandler,
  cacheSize: CacheSize,
  metrics: SinkStormMetrics,
  maxWaitingFutures: MaxWaitingFutures,
  includeSuccessHandler: IncludeSuccessHandler,
  anchor: AnchorTuples,
  shouldEmit: Boolean) extends
    AsyncBaseBolt[((Key, BatchID), Value), (Key, (Option[Value], Value))](
      metrics.metrics,
      anchor,
      maxWaitingFutures,
      shouldEmit) {

  import Constants._
  import JListSemigroup._

  val storeBox = Externalizer(storeSupplier)
  lazy val store = storeBox.get.apply

  // See MaxWaitingFutures for a todo around removing this.
  lazy val cacheCount = cacheSize.size
  lazy val buffer = SummingQueue[Map[(Key, BatchID), (JList[Tuple], Timestamp, Value)]](cacheCount.getOrElse(0))

  val exceptionHandlerBox = Externalizer(exceptionHandler.handlerFn.lift)
  val successHandlerBox = Externalizer(successHandler)

  var successHandlerOpt: Option[OnlineSuccessHandler] = null

  override val decoder = new KeyValueInjection[(Key,BatchID), Value](AGG_KEY, AGG_VALUE)
  override val encoder = new SingleItemInjection[(Key, (Option[Value], Value))](VALUE_FIELD)

  override def prepare(conf: JMap[_,_], context: TopologyContext, oc: OutputCollector) {
    super.prepare(conf, context, oc)
    successHandlerOpt = if (includeSuccessHandler.get) Some(successHandlerBox.get) else None
  }

  protected override def fail(inputs: JList[Tuple], error: Throwable): Unit = {
    super.fail(inputs, error)
    exceptionHandlerBox.get.apply(error)
    ()
  }

  override def apply(tuple: Tuple,
    tsIn: (Timestamp, ((Key, BatchID), Value))):
      Future[Iterable[(JList[Tuple], Future[TraversableOnce[(Timestamp, (Key, (Option[Value], Value)))]])]] = {

    val (ts, (kb, v)) = tsIn
    Future.value {
      // See MaxWaitingFutures for a todo around simplifying this.
      buffer(Map(kb -> ((lift(tuple), ts, v))))
        .map { kvs =>
          kvs.iterator.map { case ((k, batchID), (tups, stamp, delta)) =>
            (tups,
              store.merge(((k, batchID), delta)).map { before =>
                List((stamp, (k, (before, delta))))
              }
              .onSuccess { _ => successHandlerOpt.get.handlerFn.apply() }
            )
          }
          .toList // force, but order does not matter, so we could optimize this
        }
        .getOrElse(Nil)
    }
  }

  override def cleanup { Await.result(store.close) }
}
