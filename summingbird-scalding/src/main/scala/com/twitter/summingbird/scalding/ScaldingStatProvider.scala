package com.twitter.summingbird.scalding

import cascading.flow.FlowProcess
import com.twitter.scalding.{ RuntimeStats => ScaldingRuntimeStats }
import com.twitter.summingbird._
import scala.util.{Try => ScalaTry}


// Incrementor for Scalding Counter (Stat) 
// Returned to the Summingbird Counter object to call incrBy function in Summingbird job code
case class ScaldingCounterIncrementor(group: String, name: String, fp: FlowProcess[_]) extends CounterIncrementor {
	def incrBy(by: Long) = fp.increment(group, name, by)
}

case class ScaldingStatProvider() extends PlatformStatProvider {

  def pullInScaldingRuntimeForJobID(jobID: SummingbirdJobId): Option[FlowProcess[_]] =
    ScalaTry[FlowProcess[_]]{ScaldingRuntimeStats.getFlowProcessForUniqueId(jobID.get)}.toOption

  // Incrementor from PlatformStatProvicer
  // We use a partially applied function: if successful, ScaldingRuntimeStats.getFlowProcessForUniqueId
  // returns the FlowProcess for this job. We then create a ScaldingCounterIncrementor object that takes the
  // FlowProcess and Counter group/name and contains an incrBy function to be called from SB job
  def counterIncrementor(jobID: SummingbirdJobId, group: String, name: String): Option[ScaldingCounterIncrementor] =
    pullInScaldingRuntimeForJobID(jobID).map { flowP: FlowProcess[_] => { ScaldingCounterIncrementor(group, name, flowP) } }
}

object ScaldingRuntimeStatsProvider {
  SummingbirdRuntimeStats.addPlatformStatProvider(new ScaldingStatProvider)
}
