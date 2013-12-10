package com.twitter.summingbird.example

import com.twitter.scalding.{ Hdfs, TextLine }
import com.twitter.summingbird.batch.{ Batcher, Timestamp, BatchID }
import com.twitter.summingbird.scalding.{ InitialBatchedStore, Scalding, HBaseVersionedStore }
import com.twitter.summingbird.scalding.state.HDFSState
import com.twitter.summingbird.scalding.store.VersionedStore
import com.twitter.summingbird.{ Platform, Producer, TimeExtractor }
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Await
import java.util.Date
import org.apache.hadoop.conf.Configuration

/**
  * The following object contains code to execute a similar Scalding
  * job to the WordCount job defined in ExampleJob.scala. This job works
  * on plain text files, as opposed to tweet Status objects.
  * The example job uses a Store on top of HBase. This does require you to 
  * set up a local running hbase with zookeeper.
  * 
  * @author Josh Buffum
  * @author Riju Kallivalappil
  */

object ScaldingRunner {
  final val MillisInHour = 60 * 60 * 1000

  /**
   * Directory location to store state and read input file.
   */
  final val JobDir = "/user/mydir/wordcount"

  /**
   * pull in the serialization injections and WordCount job
   */
  import Serialization._
  
  implicit val batcher = Batcher.ofHours(1)

  // taken from ExampleJob
  def tokenize(text: String) : TraversableOnce[String] =
    text.toLowerCase
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .split("\\s+")
      
  /**
    * The actual Summingbird job. Works against text instead of tweet Status
    */
  def wordCount[P <: Platform[P]](source: Producer[P, String], store: P#Store[String, Long]) = {
    source
      .filter(_ != null)
      .flatMap { text: String => tokenize(text).map(_ -> 1L) }
      .sumByKey(store)
  }

  // Always use an hour before the current time as the batch id.
  // The storm job uses the current hour. This way we can get the "merger" to work across 2 batches
  implicit val timeOf: TimeExtractor[String] = TimeExtractor(_ => new Date().getTime - MillisInHour)

  val now = System.currentTimeMillis
  val waitingState = HDFSState(JobDir + "/waitstate", startTime = Some(Timestamp(now - 2 * MillisInHour)),
    numBatches = 3)
  
  // read text lines in input.txt as job input
  val src = Producer.source[Scalding, String](Scalding.pipeFactoryExact(_ => TextLine(JobDir + "/input.txt")))
  
  /**
   * Create the HBaseVersionedStore. Results from the Scalding job will be written
   * as String => (BatchID, Long) pairs into a HBase cluster defined in a Zookeeper
   * quorum at "localhost" in a table "wordcountJob"
   */
  val versionedStore = HBaseVersionedStore[String, Long] (
      Seq("localhost"),
      "wordcountJob"
  )
  
  /**
   * wrap the HBaseVersionedStore with an InitialBatchedStore to take care of the early batches
   */
  val store = new InitialBatchedStore(batcher.currentBatch - 2L, versionedStore)
  val mode = Hdfs(false, new Configuration())

  /**
   * main
   * Create the Scalding job and run it
   */
  def main(args: Array[String]) {
    val job =  Scalding("wordcountJob")
    job.run(waitingState, mode, job.plan(wordCount[Scalding](src, store)))
  }

  def getReadableStore(batchOffset: Int = 0)(implicit batcher: Batcher) = {
    versionedStore.asInstanceOf[ReadableStore[String, (BatchID,Long)]]
  }

  /**
   * lookup a Key value in the HBase store
   */
  def lookup(key: String) : Option[(BatchID, Long)] = {
    val reader = getReadableStore()
    
    Await.result {
      reader.get(key)
    }
  }
  
}
