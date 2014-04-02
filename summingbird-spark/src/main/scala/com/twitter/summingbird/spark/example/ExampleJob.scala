package com.twitter.summingbird.spark.example

import org.apache.spark.SparkContext
import com.twitter.summingbird.{Producer, Platform, Source}
import com.twitter.util.Await
import com.twitter.summingbird.spark.{SparkSink, SparkPlatform}
import org.apache.spark.rdd.RDD

object ExampleJob extends App {

  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("yarn-standalone", "Summingbird Spark!")

    val src = sc.textFile(args(0))
    //val store = sc.textFile(args(1))
    val store = sc.makeRDD[(String, Long)](Seq())

    val sink = new TextKeyValSink(args(2))

    val job = makeJob[SparkPlatform](src, store, sink)
    println(job)

    val plat = new SparkPlatform
    val fplan = plat.plan(job)
    val plan = Await.result(fplan)
    println(plan.toDebugString)
    val results = plan.toArray().toSeq
    println(results)
  }

  def makeJob[P <: Platform[P]](source: P#Source[String],
                                store:  P#Store[String, Long],
                                sink:   P#Sink[(String, Any)]) = {
    Source[P, String](source)
      .flatMap { _.toLowerCase.split(" ") }
      .map { _ -> 1L }
      .sumByKey(store)
      .write(sink)
  }
}

class TextKeyValSink(path: String) extends SparkSink[(String, Any)] {
  override def write(rdd: RDD[(String, Any)]): Unit = {
    rdd.map { case (k, v) => k + "|" + v }.saveAsTextFile(path)
  }
}
