package com.twitter.summingbird.spark

import org.apache.spark.rdd.RDD

/**
 * [[SparkPlatform]]'s notion of a [[com.twitter.summingbird.Platform.Sink]]
 *
 * It is safe to do blocking operations in the write method, as it will be run in its own thread.
 *
 * @author Alex Levenson
 */
trait SparkSink[T] {
  def write(rdd: RDD[T]): Unit
}