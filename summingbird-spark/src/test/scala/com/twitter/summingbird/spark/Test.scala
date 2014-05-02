package com.twitter.summingbird.spark

import org.specs2.mutable.Specification
import org.apache.spark.SparkContext
import com.twitter.summingbird.{Producer, Platform, Source}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
/*
object InMemSource {
  def of[T : ClassTag](s: Seq[T]): SparkSource[T] = {
    new SparkSource[T] {
      override def rdd(sc: SparkContext): RDD[T] = sc.makeRDD(s)
    }
  }
}

object InMemStore {
  def of[K : ClassTag, V : ClassTag](s: Seq[(K, V)]): SparkStore[K, V] = {
    new SparkStore[K, V] {
      override def rdd(sc: SparkContext): RDD[(K, V)] = sc.makeRDD(s)
    }
  }
}


class Test extends Specification {

  "example job" should {

    val src = Source[SparkPlatform, String](InMemSource.of(Seq("hello I am alex", "alex I am", "Who am I")))

    val store = InMemStore.of(Seq("hello" -> 1000L, "who" -> 3000L))

    val sink = new SparkSink[(String, (Option[Long], Long))] {
      override def write(rdd: RDD[(String, (Option[Long], Long))]): Unit = {
        println(rdd.toArray().seq)
      }
    }

    val job = makeJob[SparkPlatform](src, store, sink)
    println(job)

    val sc = new SparkContext("local", "Simple App")
    val plat = new SparkPlatform(sc)

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
*/