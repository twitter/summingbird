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

import com.twitter.summingbird._
import com.twitter.scalding.{ RichDate, DateParser, Hdfs, Args }

import com.twitter.summingbird.batch.{ Timestamp, WaitingState }
import com.twitter.summingbird.batch.option.{ FlatMapShards, Reducers }
import com.twitter.summingbird.chill.ChillExecutionConfig
import com.twitter.algebird.Interval

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser

import java.util.TimeZone
import org.slf4j.LoggerFactory

/**
 * @author Ian O Connell
 */

trait ScaldingExecutionConfig extends ChillExecutionConfig[Scalding] {
  def getWaitingState(
    hadoopConfig: Configuration,
    startDate: Option[Timestamp],
    batches: Int): WaitingState[Interval[Timestamp]]
}

object Executor {
  @transient private val logger = LoggerFactory.getLogger(Executor.getClass)

  def buildHadoopConf(inArgs: Array[String]): (Configuration, Args) = {
    val baseConfig = new Configuration
    val args = Args(new GenericOptionsParser(baseConfig, inArgs).getRemainingArgs)
    (baseConfig, args)
  }

  def apply(inArgs: Array[String], generator: (Args => ScaldingExecutionConfig)) {

    val (hadoopConf, args) = buildHadoopConf(inArgs)

    val config = generator(args)
    // Summingbird's Scalding mode needs some way to figure out the very
    // first batch to grab. This particular implementation gets the
    // --start-time option from the command line, asks the batcher for
    // the relevant Time, converts that to a Batch and sets this as the
    // initial batch to process. All runs after the first batch
    // (incremental updates) will use the batch of the previous run as
    // the starting batch, rendering this unnecessary.
    def startDate: Option[Timestamp] =
      args.optional("start-time")
        .map(RichDate(_)(TimeZone.getTimeZone("UTC"), DateParser.default).value)

    // The number of batches to process in this particular run. Imagine
    // a batch size of one hour; For big recomputations, one might want
    // to process a day's worth of data with each Hadoop run. Do this by
    // setting --batches to "24" until the recomputation's finished.
    def batches: Int = args.getOrElse("batches", "1").toInt

    def reducers: Int = args.getOrElse("reducers", "20").toInt

    // Immediately shuffles the input data into the supplied number of shards
    // Should only be used if the mapper tasks are doing heavy work
    // and would be faster to force a shuffle immediately after the data is read
    def shards: Int = args.getOrElse("shards", "0").toInt

    val options = Map("DEFAULT" -> Options().set(Reducers(reducers)).set(FlatMapShards(shards))) ++ config.getNamedOptions

    val scaldPlatform = Scalding(config.name, options)
      .withRegistrars(config.registrars)
      .withConfigUpdater { c => com.twitter.scalding.Config.tryFrom(config.transformConfig(c.toMap).toMap).get }

    val toRun = scaldPlatform.plan(config.graph)

    try {
      scaldPlatform
        .run(config.getWaitingState(hadoopConf, startDate, batches), Hdfs(true, hadoopConf), toRun)
    } catch {
      case f @ FlowPlanException(errs) =>
        /* This is generally due to data not being ready, don't give a failed error code */
        if (!args.boolean("scalding.nothrowplan")) {
          logger.error("use: --scalding.nothrowplan to not give a failing error code in this case")
          throw f
        } else {
          logger.info("[ERROR]: ========== FlowPlanException =========")
          errs.foreach { logger.info(_) }
          logger.info("========== FlowPlanException =========")
        }
    }
  }
}
