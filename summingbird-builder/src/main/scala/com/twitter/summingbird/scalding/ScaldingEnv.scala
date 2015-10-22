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

import com.twitter.scalding.{ Args, Config, Hdfs, RichDate, DateParser }
import com.twitter.summingbird.batch.store.HDFSMetadata
import com.twitter.summingbird.{ Env, Summer, TailProducer, AbstractJob }
import com.twitter.summingbird.batch.{ BatchID, Batcher, Timestamp }
import com.twitter.summingbird.builder.{ SourceBuilder, CompletedBuilder }
import com.twitter.summingbird.batch.option.Reducers
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import java.util.TimeZone

/**
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

// If present, after the groupAndSum we store
// the intermediate key-values for using a store as a service
// in another job.
// Prefer using .write in the -core API.
case class StoreIntermediateData[K, V](sink: Sink[(K, V)]) extends java.io.Serializable

// TODO (https://github.com/twitter/summingbird/issues/69): Add
// documentation later describing command-line args. start-time,
// batches, reducers, Hadoop-specific arguments and where they go. We
// might pull this argument-parsing out into its own class with all
// arguments defined to make it easier to understand (and add to
// later).

case class ScaldingEnv(override val jobName: String, inargs: Array[String])
    extends Env(jobName) {

  override lazy val args = {
    // pull out any hadoop specific args
    Args(new GenericOptionsParser(new Configuration, inargs).getRemainingArgs)
  }

  def tz = TimeZone.getTimeZone("UTC")

  // Summingbird's Scalding mode needs some way to figure out the very
  // first batch to grab. This particular implementation gets the
  // --start-time option from the command line, asks the batcher for
  // the relevant Time, converts that to a Batch and sets this as the
  // initial batch to process. All runs after the first batch
  // (incremental updates) will use the batch of the previous run as
  // the starting batch, rendering this unnecessary.
  def startDate: Option[Timestamp] =
    args.optional("start-time")
      .map(RichDate(_)(tz, DateParser.default).value)

  def initialBatch(b: Batcher): Option[BatchID] = startDate.map(b.batchOf(_))

  // The number of batches to process in this particular run. Imagine
  // a batch size of one hour; For big recomputations, one might want
  // to process a day's worth of data with each Hadoop run. Do this by
  // setting --batches to "24" until the recomputation's finished.
  def batches: Int = args.getOrElse("batches", "1").toInt

  // The number of reducers to use for the Scalding piece of the
  // Summingbird job.
  def reducers: Int = args.getOrElse("reducers", "20").toInt

  // Used to insert a write just before the store so the store
  // can be used as a Service
  private def addDeltaWrite(snode: Summer[Scalding, Any, Any],
    sink: Sink[(Any, Any)]): Summer[Scalding, Any, Any] = {
    val Summer(prod, store, monoid) = snode
    Summer(prod.write(sink), store, monoid)
  }

  case class Built(platform: Scalding,
    toRun: TailProducer[Scalding, (Any, (Option[Any], Any))],
    stateFn: (Configuration) => VersionedState)

  @transient lazy val build: Built = {
    // Calling abstractJob's constructor and binding it to a variable
    // forces any side effects caused by that constructor (building up
    // of the environment and defining the builder).
    val ajob = abstractJob
    val scaldingBuilder = builder.asInstanceOf[CompletedBuilder[Scalding, Any, Any]]
    val name = args.optional("name").getOrElse(ajob.getClass.getName)

    // Perform config transformations before Hadoop job submission
    val opts = SourceBuilder.adjust(
      scaldingBuilder.opts, scaldingBuilder.id)(_.set(Reducers(reducers)))

    // Support for the old setting based writing
    val toRun: TailProducer[Scalding, (Any, (Option[Any], Any))] =
      (for {
        opt <- opts.get(scaldingBuilder.id)
        stid <- opt.get[StoreIntermediateData[Any, Any]]
      } yield addDeltaWrite(scaldingBuilder.node, stid.sink))
        .getOrElse(scaldingBuilder.node)
        .name(scaldingBuilder.id)

    val scald = Scalding(name, opts)
      .withRegistrars(ajob.registrars ++ builder.registrar.getRegistrars.asScala)
      .withConfigUpdater { c =>
        Config.tryFrom(ajob.transformConfig(c.toMap).toMap).get
      }

    def getStatePath(ss: Store[_, _]): Option[String] =
      ss match {
        case store: store.VersionedBatchStore[_, _, _, _] => Some(store.rootPath)
        case initstore: store.InitialBatchedStore[_, _] => getStatePath(initstore.proxy)
        case _ => None
      }
    // VersionedState needs this
    implicit val batcher = scaldingBuilder.batcher
    val stateFn = { (conf: Configuration) =>
      val statePath = getStatePath(scaldingBuilder.node.store).getOrElse {
        sys.error("You must use a VersionedBatchStore with the old Summingbird API!")
      }
      VersionedState(HDFSMetadata(conf, statePath), startDate, batches)
    };

    Built(scald, toRun, stateFn)
  }

  def run() = run(build)

  def run(b: Built) {
    val Built(scald, toRun, stateFn) = b

    val conf = new Configuration
    // Add the generic options
    new GenericOptionsParser(conf, inargs)

    try {
      scald.run(stateFn(conf), Hdfs(true, conf), toRun)
    } catch {
      case f @ FlowPlanException(errs) =>
        /* This is generally due to data not being ready, don't give a failed error code */
        if (!args.boolean("scalding.nothrowplan")) {
          println("use: --scalding.nothrowplan to not give a failing error code in this case")
          throw f
        } else {
          println("[ERROR]: ========== FlowPlanException =========")
          errs.foreach { println(_) }
          println("========== FlowPlanException =========")
        }
    }
  }
}
