package com.twitter.summingbird.spark.example

import org.apache.spark.SparkContext
import com.twitter.summingbird.{Platform, Source}
import com.twitter.util.Await
import com.twitter.summingbird.spark.{SparkSink, SparkPlatform}
import org.apache.spark.rdd.{EmptyRDD, RDD}
import com.google.common.base.{CharMatcher, Splitter}
import scala.collection.JavaConversions._

object ExampleJob extends App {

  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("yarn-standalone", "Summingbird Spark!")

    val src = sc.textFile(args(0))
    val store = sc.textFile(args(1)).map { x =>
      val parts = x.split("|")
      (parts(0), parts(2))
    }

    //val store = new EmptyRDD[(String, Long)](sc)

    val sink = new TextKeyValSink(args(2))

    val job = makeJob[SparkPlatform](src, store, sink)
    println(job)

    val plat = new SparkPlatform
    val fplan = plat.plan(job)
    plat.run()
    val plan = Await.result(fplan)
    println(plan.toDebugString)
    val count = plan.count()
    //val results = plan.toArray().toSeq
    println("Count: " + count)
  }

  lazy val splitter = Splitter
    .on(CharMatcher.BREAKING_WHITESPACE.or(CharMatcher.JAVA_LETTER.negate()))
    .trimResults()
    .omitEmptyStrings()

  def makeJob[P <: Platform[P]](source: P#Source[String],
                                store:  P#Store[String, Long],
                                sink:   P#Sink[String]) = {

    Source[P, String](source)
      .flatMap { x => splitter.split(x.toLowerCase) }
      .map { _ -> 1L }
      .sumByKey(store)
      .map { case(token, (prev, delta)) => token + "|" + prev.getOrElse(0) + "|" + delta }
      .write(sink)
  }
}

class TextKeyValSink(path: String) extends SparkSink[String] {
  override def write(rdd: RDD[String]): Unit = {
    rdd.saveAsTextFile(path)
  }
}
