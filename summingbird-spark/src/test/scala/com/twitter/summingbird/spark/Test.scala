package com.twitter.summingbird.spark

import org.specs2.mutable.Specification
import org.apache.spark.SparkContext
import com.twitter.summingbird.{Producer, Platform, Source}
import org.apache.spark.rdd.RDD

class Test extends Specification {

  "example job" should {
    val sc = new SparkContext("local", "Simple App")
    val sentencesRDD = sc.makeRDD(Seq("hello I am alex", "alex I am", "Who am I"))
    val src = Source[SparkPlatform, String](sentencesRDD)
    val store = sc.makeRDD(Seq("hello" -> 1000L, "who" -> 3000L))

    val sink = new SparkSink[(String, (Option[Long], Long))] {
      override def write(rdd: RDD[(String, (Option[Long], Long))]): Unit = {
        println(rdd.toArray().seq)
      }
    }

    val job = makeJob[SparkPlatform](src, store, sink)
    println(job)

    val plat = new SparkPlatform
    val plan = plat.plan(job)
    println(plan.toDebugString)
    plat.run()
  }

  def makeJob[P <: Platform[P]](source: Producer[P, String], store: P#Store[String, Long], sink: P#Sink[(String, (Option[Long], Long))]) = {
    source
      .flatMap { _.toLowerCase.split(" ") }
      .map { _ -> 1L }
      .sumByKey(store)
      .write(sink)
  }
}