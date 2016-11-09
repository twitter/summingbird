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

import com.twitter.storehaus.algebra.reporting.{ MergeableReporter, StoreReporter }
import com.twitter.storehaus.algebra.{ Mergeable, MergeableProxy }
import com.twitter.storehaus.{ Store, StoreProxy }
import com.twitter.util.Future
import org.apache.storm.metric.api.CountMetric
import org.apache.storm.task.TopologyContext

/**
 *
 * @author Ian O Connell
 */

class MergeableStatReporter[K, V](context: TopologyContext, val self: Mergeable[K, V]) extends MergeableProxy[K, V] with MergeableReporter[Mergeable[K, V], K, V] {
  private def buildMetric(s: String) = context.registerMetric("store/%s".format(s), new CountMetric, 10)
  val mergeMetric = buildMetric("merge")
  val multiMergeMetric = buildMetric("multiMerge")
  val multiMergeTupleFailedMetric = buildMetric("multiMergeTupleFailed")
  val mergeFailedMetric = buildMetric("mergeFailed")
  val multiMergeTuplesMetric = buildMetric("multiMergeTuples")

  override def traceMerge(kv: (K, V), request: Future[Option[V]]) = {
    mergeMetric.incr()
    request.onFailure { _ =>
      mergeFailedMetric.incr()
    }.unit
  }

  override def traceMultiMerge[K1 <: K](kvs: Map[K1, V], request: Map[K1, Future[Option[V]]]) = {
    multiMergeMetric.incr()
    multiMergeTuplesMetric.incrBy(request.size)
    request.map {
      case (k, v) =>
        val failureWrapV = v.onFailure { _ =>
          multiMergeTupleFailedMetric.incr()
        }.unit
        (k, failureWrapV)
    }
  }
}

class StoreStatReporter[K, V](context: TopologyContext, val self: Store[K, V]) extends StoreProxy[K, V] with StoreReporter[Store[K, V], K, V] {
  private def buildMetric(s: String) = context.registerMetric("store/%s".format(s), new CountMetric, 10)
  val putMetric = buildMetric("put")
  val multiPutMetric = buildMetric("multiPut")
  val multiPutTuplesMetric = buildMetric("multiPutTuples")
  val putFailedMetric = buildMetric("putFailed")
  val multiPutTupleFailedMetric = buildMetric("multiPutTupleFailed")

  val getMetric = buildMetric("get")
  val multiGetMetric = buildMetric("multiGet")
  val multiGetTuplesMetric = buildMetric("multiGetTuples")
  val getFailedMetric = buildMetric("getFailed")
  val multiGetTupleFailedMetric = buildMetric("multiGetTupleFailed")

  override def traceMultiGet[K1 <: K](ks: Set[K1], request: Map[K1, Future[Option[V]]]) = {
    multiGetMetric.incr()
    multiGetTuplesMetric.incrBy(request.size)

    request.map {
      case (k, v) =>
        val failureWrapV = v.onFailure { _ =>
          multiGetTupleFailedMetric.incr()
        }.unit
        (k, failureWrapV)
    }
  }

  override def traceGet(k: K, request: Future[Option[V]]) = {
    getMetric.incr()
    request.onFailure { _ =>
      getFailedMetric.incr()
    }.unit
  }

  override def tracePut(kv: (K, Option[V]), request: Future[Unit]) = {
    putMetric.incr()
    request.onFailure { _ =>
      putFailedMetric.incr()
    }.unit
  }

  override def traceMultiPut[K1 <: K](kvs: Map[K1, Option[V]], request: Map[K1, Future[Unit]]) = {
    multiPutMetric.incr()
    multiPutTuplesMetric.incrBy(request.size)

    request.map {
      case (k, v) =>
        val failureWrapV = v.onFailure { _ =>
          multiPutTupleFailedMetric.incr()
        }.unit
        (k, failureWrapV)
    }
  }
}
