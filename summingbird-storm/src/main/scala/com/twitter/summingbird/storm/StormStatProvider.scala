package com.twitter.summingbird.storm

import com.twitter.summingbird.{ CounterIncrementor, Group, Name, PlatformStatProvider }
import com.twitter.summingbird.option.JobId
import com.twitter.util.{ Promise, Await }
import java.util.concurrent.ConcurrentHashMap
import org.apache.storm.metric.api.CountMetric
import org.apache.storm.task.TopologyContext
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

// Incrementor for Storm Counters
// Returned to the Summingbird Counter object to call incrBy function in SB job code
private[summingbird] case class StormCounterIncrementor(metric: CountMetric) extends CounterIncrementor {
  def incrBy(by: Long): Unit = metric.incrBy(by)
}

// StormStatProvider global object that contains counter information for the Storm job(s)
private[summingbird] object StormStatProvider extends PlatformStatProvider {
  @transient private val logger = LoggerFactory.getLogger(StormStatProvider.getClass)

  // Keep a HashMap of JobId->Promise Map[String, CountMetric] where String is the Counter name
  // Each metrics map will be initialized once (via Promise) and then referred to by other Bolts
  private val metricsForJob = new ConcurrentHashMap[JobId, ConcurrentHashMap[String, CountMetric]]

  def registerMetrics(jobID: JobId,
    context: TopologyContext,
    metrics: Seq[(Group, Name)]) {

    metricsForJob.putIfAbsent(jobID, new ConcurrentHashMap[String, CountMetric])
    val jobMap = metricsForJob.get(jobID)

    metrics.foreach {
      case (groupName, metricName) =>

        val k = groupName.getString + "/" + metricName.getString
        val v = new CountMetric

        if (jobMap.putIfAbsent(k, v) == null) {
          logger.info(s"Registered metric $k with TopologyContext")
          context.registerMetric(k, v, 60)
        } // otherwise another bolt on the same jvm has beaten us to registering this.
    }
  }

  // returns Storm counter incrementor to the Counter object in Summingbird job
  def counterIncrementor(jobID: JobId, group: Group, name: Name): Option[StormCounterIncrementor] =
    Option(metricsForJob.get(jobID)).map { m =>
      StormCounterIncrementor(m.asScala.getOrElse(group.getString + "/" + name.getString,
        sys.error(s"It is only valid to create counter objects during job submission, tried to find $jobID -> $group/$name ")))
    }
}
