///*
//Copyright 2013 Twitter, Inc.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//*/
//
//package com.twitter.summingbird.akka
//
//import com.twitter.algebird.Monoid
//import com.twitter.storehaus.algebra.MergeableStore
//import com.twitter.summingbird.batch.BatchID
//import com.twitter.util.Future
//
///**
//  * CollectorMergeableStore merges (K, BatchID) -> V into the
//  * underlying store by way of a storm topology.
//  *
//  * The summingbird storm topology is really just a Storehaus
//  * MergeableStore that shards the various "merge" calls by key. This
//  * brings us a little closer to expressing that idea, and lets us use
//  * the "BufferingStore" combinator on the FlatMapBolt collector
//  * itself without maintaining a separate SummingQueue in the
//  * FlatMapBolt and recreating the logic. 
//  *
//  * @author Sam Ritchie
//  */
//
//class CollectorMergeableStore[K, V]()
//  (override implicit val monoid: Monoid[V])
//    extends MergeableStore[(K, BatchID), V] {
//  override def get(k: (K, BatchID)) =
//    sys.error("Gets out of a CollectorMergeableStore are not supported.")
//  override def put(pair: ((K, BatchID), Option[V])) =
//    sys.error("Puts into a CollectorMergeableStore are not supported.")
//
//  override def merge(pair: ((K, BatchID), V)) = {
//    val ((k, tuple, id), v) = pair
//    val values = new Values(
//      id.asInstanceOf[AnyRef],
//      k.asInstanceOf[AnyRef],
//      v.asInstanceOf[AnyRef]
//    )
//	
//	//collector.emit(values)
//    Future.Unit
//  }
//}
