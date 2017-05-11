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

package com.twitter.summingbird.example

import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.Options
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.online.MergeableStoreFactory
import com.twitter.summingbird.storm.{ Storm, Executor, StormExecutionConfig }
import com.twitter.summingbird.online.option.{ FlatMapParallelism, SummerParallelism, SourceParallelism }
import org.apache.storm.{ Config => BTConfig }
import com.twitter.scalding.Args
import com.twitter.tormenta.spout.TwitterSpout
import com.twitter.util.Await
import twitter4j.TwitterStreamFactory
import twitter4j.conf.ConfigurationBuilder

object ExeStorm {
  def main(args: Array[String]) {
    Executor(args, StormRunner(_))
  }
}

/**
 * The following object contains code to execute the Summingbird
 * WordCount job defined in ExampleJob.scala on a storm
 * cluster.
 */
object StormRunner {
  /**
   * These imports bring the requisite serialization injections, the
   * time extractor and the batcher into implicit scope. This is
   * required for the dependency injection pattern used by the
   * Summingbird Storm platform.
   */
  import Serialization._, StatusStreamer._

  /**
   * Configuration for Twitter4j. Configuration can also be managed
   * via a properties file, as described here:
   *
   * http://tugdualgrall.blogspot.com/2012/11/couchbase-create-large-dataset-using.html
   */
  lazy val config = new ConfigurationBuilder()
    .setOAuthConsumerKey("mykey")
    .setOAuthConsumerSecret("mysecret")
    .setOAuthAccessToken("token")
    .setOAuthAccessTokenSecret("tokensecret")
    .setJSONStoreEnabled(true) // required for JSON serialization
    .build

  /**
   * "spout" is a concrete Storm source for Status data. This will
   * act as the initial producer of Status instances in the
   * Summingbird word count job.
   */
  val spout = TwitterSpout(new TwitterStreamFactory(config))

  /**
   * And here's our MergeableStore supplier.
   *
   * A supplier is required (vs a bare store) because Storm
   * serializes every constructor parameter to its
   * "bolts". Serializing a live memcache store is a no-no, so the
   * Storm platform accepts a "supplier", essentially a function0
   * that when called will pop out a new instance of the required
   * store. This instance is cached when the bolt starts up and
   * starts merging tuples.
   *
   * A MergeableStore is a store that's aware of aggregation and
   * knows how to merge in new (K, V) pairs using a Monoid[V]. The
   * Monoid[Long] used by this particular store is being pulled in
   * from the Monoid companion object in Algebird. (Most trivial
   * Monoid instances will resolve this way.)
   *
   * First, the backing store:
   */
  lazy val stringLongStore =
    Memcache.mergeable[(String, BatchID), Long]("urlCount")

  /**
   * the param to store is by name, so this is still not created created
   * yet
   */
  val storeSupplier: MergeableStoreFactory[(String, BatchID), Long] = Storm.store(stringLongStore)

  /**
   * This function will be called by the storm runner to request the info
   * of what to run. In local mode it will start up as a
   * separate thread on the local machine, pulling tweets off of the
   * TwitterSpout, generating and aggregating key-value pairs and
   * merging the incremental counts in the memcache store.
   *
   * Before running this code, make sure to start a local memcached
   * instance with "memcached". ("brew install memcached" will get
   * you all set up if you don't already have memcache installed
   * locally.)
   */

  def apply(args: Args): StormExecutionConfig = {
    new StormExecutionConfig {
      override val name = "SummingbirdExample"

      // No Ackers
      override def transformConfig(config: Map[String, AnyRef]): Map[String, AnyRef] = {
        config ++ List((BTConfig.TOPOLOGY_ACKER_EXECUTORS -> (new java.lang.Integer(0))))
      }

      override def getNamedOptions: Map[String, Options] = Map(
        "DEFAULT" -> Options().set(SummerParallelism(2))
          .set(FlatMapParallelism(80))
          .set(SourceParallelism(16))
          .set(CacheSize(100))
      )
      override def graph = wordCount[Storm](spout, storeSupplier)
    }
  }

  /**
   * Once you've got this running in the background, fire up another
   * repl and query memcached for some counts.
   *
   * The following commands will look up words. Hitting a word twice
   * will show that Storm is updating Memcache behind the scenes:
   * {{{
   * scala>     lookup("i") // Or any other common word
   * res7: Option[Long] = Some(1774)
   *
   * scala>     lookup("i")
   * res8: Option[Long] = Some(1779)
   * }}}
   */
  def lookup(word: String): Option[Long] =
    Await.result {
      stringLongStore.get(word -> StatusStreamer.batcher.currentBatch)
    }
}
