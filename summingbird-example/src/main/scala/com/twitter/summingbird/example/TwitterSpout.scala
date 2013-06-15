/*
 Copyright 2012 Twitter, Inc.

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

package com.twitter.summingbird.example

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Fields, Values}
import backtype.storm.utils.Time
import com.twitter.tormenta.spout.Spout
import java.util.{Map => JMap}
import java.util.concurrent.LinkedBlockingQueue
import twitter4j._

/**
  * Storm Spout implementation for Twitter's streaming API.
  *
  * TODO: Replace with tormenta's version on the next publish.
  *
  * @author Sam Ritchie
  */
object TwitterSpout {
  val QUEUE_LIMIT = 1000 // default max queue size.
  val FIELD_NAME = "tweet" // default output field name.

  def apply(
    factory: TwitterStreamFactory,
    limit: Int = QUEUE_LIMIT,
    fieldName: String = FIELD_NAME): TwitterSpout[Status] =
    new TwitterSpout(factory, limit, fieldName)(i => Some(i))
}

class TwitterSpout[+T](factory: TwitterStreamFactory, limit: Int, fieldName: String)(fn: Status => TraversableOnce[T])
    extends BaseRichSpout with Spout[T] {

  var stream: TwitterStream = null
  var collector: SpoutOutputCollector = null

  lazy val queue = new LinkedBlockingQueue[Status](limit)
  lazy val listener = new StatusListener {
    def onStatus(status: Status) {
      queue.offer(status)
    }
    def onDeletionNotice(notice: StatusDeletionNotice) { }
    def onScrubGeo(userId: Long, upToStatusId: Long) { }
    def onStallWarning(warning: StallWarning) { }
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int) { }
    def onException(ex: Exception) { }
  }

  override def getSpout = this

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields(fieldName))
  }

  override def open(conf: JMap[_, _], context: TopologyContext, coll: SpoutOutputCollector) {
    collector = coll
    stream = factory.getInstance
    stream.addListener(listener)

    // TODO: Add support beyond "sample". (GardenHose, for example.)
    stream.sample
  }

  /**
    * Override this to change the default spout behavior if poll
    * returns an empty list.
    */
  def onEmpty: Unit = Time.sleep(50)

  override def nextTuple {
    Option(queue.poll).map(fn).flatten match {
      case Nil => onEmpty
      case items => items.foreach { item =>
        collector.emit(new Values(item.asInstanceOf[AnyRef]))
      }
    }
  }

  override def flatMap[U](newFn: T => TraversableOnce[U]) =
    new TwitterSpout(factory, limit, fieldName)(fn(_).flatMap(newFn))

  override def close { stream.shutdown }
}
