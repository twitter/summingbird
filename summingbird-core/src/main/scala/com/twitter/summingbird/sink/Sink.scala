package com.twitter.summingbird.sink

import cascading.flow.FlowDef

import com.twitter.scalding.{Mode, TypedPipe}
import com.twitter.summingbird.scalding.ScaldingEnv
import com.twitter.util.Future

/**
 * Represents a location to which intermediate results of the "flatMap" operation
 * can be written for consumption by other jobs. On the offline side, this can be a
 * time-based source on HDFS with one file per each batch ID. On the online side, this
 * can be a kestrel fanout or kafka topic.
 */

trait OfflineSink[Event] {
  def write(pipe: TypedPipe[Event])
    (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv)
}

class EmptyOfflineSink[Event] extends OfflineSink[Event] {
  def write(pipe: TypedPipe[Event])
    (implicit fd: FlowDef, mode: Mode, env: ScaldingEnv) {}
}

trait OnlineSink[Event] {
  /**
   * Note that the flatMap operation WILL error if this future errors, so be sure
   * to handle appropriate exceptions here.
   */
  def write(event: Event): Future[Unit]
}

class EmptyOnlineSink[Event] extends OnlineSink[Event] {
  def write(event: Event) = Future.Unit
}

case class CompoundSink[Event](offline: OfflineSink[Event], online: () => OnlineSink[Event])

object CompoundSink {
  def fromOffline[Event](offline: OfflineSink[Event]): CompoundSink[Event] =
    CompoundSink(offline, () => new EmptyOnlineSink[Event]())

  def fromOnline[Event](online: => OnlineSink[Event]): CompoundSink[Event] =
    CompoundSink(new EmptyOfflineSink[Event](), () => online)
}