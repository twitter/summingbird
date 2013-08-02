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

package com.twitter.summingbird.scalding

import com.twitter.bijection.Conversion.asMethod
import com.twitter.scalding.{ Tool => STool, _ }
import com.twitter.summingbird.scalding.store.HDFSMetadata
import com.twitter.summingbird.{ Env, Unzip2, Summer }
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.builder.{ SourceBuilder, Reducers }
import com.twitter.summingbird.storm.Storm
import com.twitter.summingbird.kryo.KryoRegistrationHelper
import com.twitter.summingbird.scalding.store.VersionedState
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.GenericOptionsParser
import java.util.{ Date, HashMap => JHashMap, Map => JMap, TimeZone }

import ConfigBijection._

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// TODO (https://github.com/twitter/summingbird/issues/69): Add
// documentation later describing command-line args. initial-run,
// start-time, batches, reducers, Hadoop-specific arguments and where
// they go. We might pull this argument-parsing out into its own class
// with all arguments defined to make it easier to understand (and add
// to later).

case class ScaldingEnv(override val jobName: String, inargs: Array[String])
    extends Env(jobName) {

  override lazy val args = {
    // pull out any hadoop specific args
    Args(new GenericOptionsParser(new Configuration, inargs).getRemainingArgs)
  }

  lazy val tz = TimeZone.getTimeZone("UTC")

  // Summingbird's Scalding mode needs some way to figure out the very
  // first batch to grab. This particular implementation gets the
  // --start-time option from the command line, asks the batcher for
  // the relevant Time, converts that to a Batch and sets this as the
  // initial batch to process. All runs after the first batch
  // (incremental updates) will use the batch of the previous run as
  // the starting batch, rendering this unnecessary.
  def startDate: Option[Date] =
    if (args.boolean("initial-run"))
      Some(args("start-time"))
        .map { dateString => DateOps.stringToRichDate(dateString)(tz).value }
    else None

  def initialBatch(b: Batcher): Option[BatchID] = startDate.map(b.batchOf(_))

  // The number of batches to process in this particular run. Imagine
  // a batch size of one hour; For big recomputations, one might want
  // to process a day's worth of data with each Hadoop run. Do this by
  // setting --batches to "24" until the recomputation's finished.
  def batches : Int = args.getOrElse("batches","1").toInt

  // The number of reducers to use for the Scalding piece of the
  // Summingbird job.
  def reducers : Int = args.getOrElse("reducers","20").toInt

  def run {
    // Calling abstractJob's constructor and binding it to a variable
    // forces any side effects caused by that constructor (building up
    // of the environment and defining the builder).
    val ajob = abstractJob
    // Perform config transformations before Hadoop job submission
    val opts = SourceBuilder.adjust(
      builder.opts, builder.id)(_.set(Reducers(reducers)))

    implicit val batcher = builder.batcher

    val scaldingSummer = builder.node.asInstanceOf[Summer[Scalding, _, _]]

    def getStatePath[K,V](ss: ScaldingStore[K, V]): Option[String] =
      ss match {
        case store: VersionedBatchStore[_, _, _, _] => Some(store.rootPath)
        case initstore: InitialBatchedStore[_, _] => getStatePath(initstore.proxy)
        case _ => None
      }

    val statePath = getStatePath(scaldingSummer.store).getOrElse {
      sys.error("You must use a VersionedBatchStore with the old Summingbird API!")
    }

    val stateMaker = { conf: Configuration =>
      VersionedState(HDFSMetadata(conf, statePath), startDate, batches)
    }

    new Scalding(
      abstractJob.getClass.getName,
      stateMaker,
      Mode.mode,
      options = opts).withConfigUpdater { conf =>
      val codecPairs = Seq(builder.keyCodecPair, builder.valueCodecPair)
      val eventCodecPairs = builder.eventCodecPairs

      // TODO: Deal with duplication here with the lift between this
      // code and SummingbirdKryoHadoop
      val jConf: JMap[String,AnyRef] = new JHashMap(fromJavaMap.invert(conf))
      KryoRegistrationHelper.registerInjections(jConf, eventCodecPairs)

      // Register key and value types. All extensions of either of these
      // types will be caught by the registered injection.
      KryoRegistrationHelper.registerInjectionDefaults(jConf, codecPairs)
      fromMap(ajob.transformConfig(jConf.as[Map[String, AnyRef]]))
    }.run(scaldingSummer)
  }
}
