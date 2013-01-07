package com.twitter.summingbird.scalding

import com.twitter.scalding.{ Args, RichDate, DateOps, DateRange, Tool => STool }
import com.twitter.summingbird.Env
import com.twitter.summingbird.batch.{ BatchID, Batcher }
import com.twitter.summingbird.util.KryoRegistrationHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.GenericOptionsParser
import java.util.{ Map => JMap }

import ConfigBijection.fromJavaMap

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// TODO: Add documentation later describing command-line
// args. initial-run, start-time, batches, reducers, Hadoop-specific
// arguments and where they go. We might pull this argument-parsing
// out into its own class with all arguments defined to make it easier
// to understand (and add to later).

case class ScaldingEnv(override val jobName: String, inargs: Array[String])
extends Env(jobName) {

  // Configuration isn't serializable, so mark this field as
  // transient. Access to this config copy should only occur at job
  // submission time, so this should be fine.
  @transient lazy val config = {
    val flatMappedBuilder = builder.flatMappedBuilder
    val codecPairs = Seq(builder.keyCodecPair, builder.valueCodecPair)

    // TODO: Deal with duplication here with the lift between this
    // code and SummingbirdKryoHadoop
    val jConf: JMap[String,AnyRef] = fromJavaMap.invert(new Configuration)
    KryoRegistrationHelper.registerBijections(jConf, flatMappedBuilder.eventCodecPairs)

    // Register key and value types. All extensions of either of these
    // types will be caught by the registered bijection.
    KryoRegistrationHelper.registerBijectionDefaults(jConf, codecPairs)
    fromJavaMap(jConf)
  }

  override lazy val args = {
    // pull out any hadoop specific args
    Args((new GenericOptionsParser(config, inargs))
      .getRemainingArgs)
  }

  // Summingbird's Scalding mode needs some way to figure out the very
  // first batch to grab. This particular implementation gets the
  // --start-time option from the command line, asks the batcher for
  // the relevant Time, converts that to a Batch and sets this as the
  // initial batch to process. All runs after the first batch
  // (incremental updates) will use the batch of the previous run as
  // the starting batch, rendering this unnecessary.
  def startBatch[Time](batcher: Batcher[Time]): Option[BatchID] =
    if (args.boolean("initial-run"))
      Some(args("start-time"))
        .map { opt => batcher.batchOf(batcher.parseTime(opt)) }
    else
      None

  // The number of batches to process in this particular run. Imagine
  // a batch size of one hour; For big recomputations, one might want
  // to process a day's worth of data with each Hadoop run. Do this by
  // setting --batches to "24" until the recomputation's finished.
  def batches : Int = args.getOrElse("batches","1").toInt

  // The number of reducers to use for the Scalding piece of the
  // Summingbird job.
  def reducers : Int = args.getOrElse("reducers","20").toInt

  @transient
  protected lazy val hadoopTool: STool = {
    val tool = new STool
    tool.setJobConstructor { jobArgs => builder.buildScalding(this) }
    tool
  }

  def run {
    // Calling abstractJob's constructor and binding it to a variable
    // forces any side effects caused by that constructor (building up
    // of the environment and defining the builder).
    val ajob = abstractJob

    // Perform config transformations before Hadoop job submission
    val finalConf = {
      val bij = ConfigBijection.fromMap
      val c = bij.invert(config)
      bij(ajob.transformConfig(c))
    }

    // Now actually run (by building the scalding job):
    ToolRunner.run(finalConf, hadoopTool, inargs);
  }
}
