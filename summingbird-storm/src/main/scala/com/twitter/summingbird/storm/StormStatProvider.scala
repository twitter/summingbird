package com.twitter.summingbird.storm

import backtype.storm.metric.api.CountMetric
import backtype.storm.task.TopologyContext
import com.twitter.summingbird.{ CounterIncrementor, PlatformStatProvider }
import com.twitter.summingbird.option.JobId
import com.twitter.util.{ Promise, Await }
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.LoggerFactory

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
  private val metricsForJob = new ConcurrentHashMap[JobId, Promise[Map[String, CountMetric]]]()

  def registerMetrics(jobID: JobId,
    context: TopologyContext,
    metrics: Seq[(String, String)]) {

    val metricsPromise = Promise[Map[String, CountMetric]]

    if (metricsForJob.putIfAbsent(jobID, metricsPromise) == null) {
      val stormMetrics = metrics.map {
        case (groupName, metricName) =>
          (groupName + "/" + metricName, new CountMetric)
      }.toMap
      logger.debug("Stats for this Bolt: {}", stormMetrics.keySet.mkString)

      // Register metrics with the Storm TopologyContext
      stormMetrics.foreach {
        case (name, metric) =>
          logger.info("Registered metric {} with TopologyContext", name)
          context.registerMetric(name, metric, 60)
      }
      // fullfill Promise
      metricsPromise.setValue(stormMetrics)
    }
  }

  // returns Storm counter incrementor to the Counter object in Summingbird job
  def counterIncrementor(passedJobId: JobId, group: String, name: String): Option[StormCounterIncrementor] =
    if (metricsForJob.containsKey(passedJobId)) {
      // Get the stormMetrics once Promise was fullfilled
      val stormMetrics = Await.result(metricsForJob.get(passedJobId))
      val metric = stormMetrics.getOrElse(group + "/" + name, sys.error("It is only valid to create counter objects during submission"))
      Some(StormCounterIncrementor(metric))
    } else {
      None
    }
}
