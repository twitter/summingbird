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

package com.twitter.summingbird.source

import backtype.storm.topology.IRichSpout

import cascading.flow.FlowDef

import com.twitter.bijection.Bijection
import com.twitter.scalding.Mode
import com.twitter.scalding.TypedPipe
import com.twitter.summingbird.batch.{ Batcher, BatchID }
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.summingbird.storm.StormEnv
import com.twitter.tormenta.spout.ScalaSpout

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// A summingbird source can pull events from a Scalding source
// (generically, an OfflineSource), a Storm spout (an OnlineSource) or
// some combination of the two. Both offline and online sources extend
// the following interface:

trait TimeExtractor[Event,Time] extends java.io.Serializable {
  def eventCodec: Bijection[Event,Array[Byte]]
  def timeCodec: Bijection[Time,Array[Byte]]
  def timeOf(e : Event) : Time
}

// Each Event produced by the source is paired with a corresponding
// Time; the Time type allows Summingbird to assign a BatchID to each
// value stored in Summingbird’s online and offline sinks (discussed
// below) and merge these values back together again in the client
// without any double counting.

// The OfflineSource[Event,Time] trait has a single method:

trait OfflineSource[Event,Time] extends TimeExtractor[Event,Time] {
  def scaldingSource(batcher : Batcher[Time], lowerb : BatchID, env : ScaldingEnv)
  (implicit flow: FlowDef, mode: Mode)
  : TypedPipe[Event]
}

// OfflineSource[Event,Time] knows how to produce a scalding Pipe
// representing all the Event objects associated with a given
// BatchID. The scaldingSource method is responsible for returning a
// TypedPipe[Event] that contains no events in the BatchID prior to
// the lowerBound.
//
// At Twitter, a typical OfflineSource implementation will map this
// lowerBound to a timestamp and use the ScaldingEnv to figure out how
// much data beyond the lowerBound to process for a given run. Since
// the HDFS log timestamps typically lag a few hours behind the
// timestamps inside the scribed events, a Twitter
// OfflineSource[Event,Time] will scoop up a few extra hours of data
// on each side of the time range calculated off of the BatchIDs and
// filter out events that don’t belong inside the range. (If the data
// in HDFS skews more than a couple of hours, this approach can miss
// data, but this is a weakness of all current Hadoop data pipelines
// at Twitter and not one that Summingbird aims to solve.)
//
// The OnlineSource[Event,Time] method is similar, but tailored to Storm:

trait OnlineSource[Event,Time] extends TimeExtractor[Event,Time] {
  def spout(env : StormEnv): ScalaSpout[Event]
  def spoutParallelism(env : StormEnv) : Int
}

// No filtering of Events is necessary here, as the spout hooks up to
// a queue which produces realtime events that are always expected to
// arrive later than the data in HDFS. The spoutParallelism is used by
// Summingbird to tune the number of Storm processes that share the
// underlying queue’s load.

// StormSource[T,Time] is a convenient type that allows the spout and
// spoutParallelism implementations to pull from the spout itself.

abstract class StormSource[T,Time](theSpout: ScalaSpout[T]) extends OnlineSource[T,Time] {
  override def spout(env : StormEnv) = theSpout
  override def spoutParallelism(env : StormEnv) = theSpout.parallelism
}

// An EventSource[Event,Time] is a compound source that mixes together
// the notions of an online and offline source:

trait EventSource[Event,Time] extends OnlineSource[Event,Time] with OfflineSource[Event,Time]

// Most of the Twitter sources we plan to implement will be
// EventSources, with knowledge of offline data in HDFS and some form
// of queue that can be wrapped by Storm.

// This object provides implicit conversions from online-only and
// offline-only sources to EventSource[Event,Time].

object EventSource {
  implicit def onlineToEvent[Event,Time](src: OnlineSource[Event,Time]) : EventSource[Event,Time] = {
    new EventSource[Event,Time] {
      lazy val offline = {
        if(src.isInstanceOf[OfflineSource[Event,Time]]) {
          Some(src.asInstanceOf[OfflineSource[Event,Time]])
        }
        else {
          None
        }
      }
      override def eventCodec = src.eventCodec
      override def timeCodec = src.timeCodec
      override def timeOf(e: Event) = src.timeOf(e)
      override def spout(env : StormEnv) = src.spout(env)
      override def spoutParallelism(env : StormEnv) = src.spoutParallelism(env)
      override def scaldingSource(batcher: Batcher[Time], lowerb: BatchID, env: ScaldingEnv)
      (implicit flow: FlowDef, mode: Mode) =
        offline.get.scaldingSource(batcher, lowerb, env)
    }
  }

  implicit def offlineToEvent[Event,Time](src: OfflineSource[Event,Time]) : EventSource[Event,Time] = {
    new EventSource[Event,Time] {
      lazy val online = {
        if(src.isInstanceOf[OnlineSource[Event,Time]])
          Some(src.asInstanceOf[OnlineSource[Event,Time]])
        else
          None
      }
      override def eventCodec = src.eventCodec
      override def timeCodec = src.timeCodec
      override def timeOf(e: Event) = src.timeOf(e)
      override def spout(env : StormEnv) = online.get.spout(env)
      override def spoutParallelism(env : StormEnv) = online.get.spoutParallelism(env)
      override def scaldingSource(batcher: Batcher[Time], lowerb: BatchID, env: ScaldingEnv)
      (implicit flow: FlowDef, mode: Mode) =
        src.scaldingSource(batcher, lowerb, env)
    }
  }
}
