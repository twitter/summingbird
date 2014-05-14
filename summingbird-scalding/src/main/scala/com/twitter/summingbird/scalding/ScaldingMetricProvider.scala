package com.twitter.summingbird.scalding

import cascading.flow.FlowProcess
import com.twitter.scalding.{ RuntimeStats => ScaldingRuntimeStats }
import com.twitter.summingbird._
import scala.util.{Try => ScalaTry}


// Incrementor for Scalding Stats
// Returned to the Summingbird Stats object to call incrBy function in SB job code
case class ScaldingStatIncrementor(group: String, name: String, fp: FlowProcess[_]) extends StatIncrementor {
	def incrBy(by: Long) = fp.increment(group, name, by)
}

case class ScaldingMetricProvider() extends PlatformMetricProvider {

  def pullInScaldingRuntimeForJobID(jobID: SummingbirdJobID): Option[FlowProcess[_]] =
    ScalaTry[FlowProcess[_]]{ScaldingRuntimeStats.getFlowProcessForUniqueId(jobID.get)}.toOption

  // Incrementor from PlatformMetricProvicer
  // We use a partially applied function: if successful, ScaldingRuntimeStats.getFlowProcessForUniqueId
  // returns the FlowProcess for this job. We then create a ScaldingStatIncrementor object that takes the
  // FlowProcess and Stat group/name and contains an incrBy function to be called from SB job
  def incrementor(jobID: SummingbirdJobID, group: String, name: String): Option[ScaldingStatIncrementor] =
    pullInScaldingRuntimeForJobID(jobID).map { flowP: FlowProcess[_] => { ScaldingStatIncrementor(group, name, flowP) } }
}

object ScaldingRuntimeStatsProvider {
  SBRuntimeStats.addPlatformMetricProvider(new ScaldingMetricProvider)
}
