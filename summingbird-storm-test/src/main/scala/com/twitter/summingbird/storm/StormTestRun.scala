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

package com.twitter.summingbird.storm

import com.twitter.algebird.Semigroup
import org.apache.storm.{ LocalCluster, Config => BacktypeStormConfig }
import com.twitter.summingbird.online.executor.InflightTuples
import org.apache.storm.testing.{ CompleteTopologyParam, MockedSources }
import com.twitter.summingbird.storm.spout.TraversableSpout
import com.twitter.summingbird.online.option._
import com.twitter.summingbird.option._
import com.twitter.summingbird._
import java.security.Permission
import com.twitter.util.Duration
import org.apache.storm.utils.Time

/**
 * This stops Storm's exception handling triggering an exit(1)
 */
private[storm] class MySecurityManager extends SecurityManager {
  override def checkExit(status: Int): Unit = {
    throw new SecurityException()
  }
  override def checkAccess(t: Thread): Unit = {}
  override def checkPermission(p: Permission): Unit = {}
  override def checkPermission(perm: Permission, context: scala.Any): Unit = {}
}

/*
 * This is a wrapper to run a storm topology.
 * We use the SecurityManager code to catch the System.exit storm calls when it
 * fails. We wrap it into a normal exception instead so it can report better/retry.
 */

object StormTestRun {
  private def completeTopologyParam(conf: BacktypeStormConfig) = {
    val ret = new CompleteTopologyParam()
    ret.setMockedSources(new MockedSources)
    ret.setStormConf(conf)
    ret.setCleanupState(false)
    ret
  }

  private def tryRun(plannedTopology: PlannedTopology): Unit = {
    //Before running the external Command
    val oldSecManager = System.getSecurityManager
    System.setSecurityManager(new MySecurityManager())
    InflightTuples.reset()
    try {
      Time.startSimulating()
      val cluster = new LocalCluster()
      cluster.submitTopology("test topology", plannedTopology.config, plannedTopology.topology)
      sleepInSimulation(7000)
      cluster.killTopology("test topology")
      sleepInSimulation(1500)
      cluster.shutdown
    } finally {
      Time.stopSimulating()
      System.setSecurityManager(oldSecManager)
    }
    require(InflightTuples.get == 0, "Inflight tuples is: %d".format(InflightTuples.get))
  }

  def sleepInSimulation(millis: Int): Unit = {
    val quantification = 10
    0.to(millis / quantification).foreach { iter =>
      Thread.sleep(quantification)
      Time.advanceTime(quantification)
    }
  }

  def apply(graph: TailProducer[Storm, Any])(implicit storm: Storm) {
    val topo = storm.plan(graph)
    apply(topo)
  }

  def simpleRun[T, K, V: Semigroup](original: List[T], mkJob: (Producer[Storm, T], Storm#Store[K, V]) => TailProducer[Storm, Any]): TestStore[K, V] = {

    implicit def extractor[T]: TimeExtractor[T] = TimeExtractor(_ => 0L)

    val (id, store) = TestStore.createStore[K, V]()

    val job = mkJob(
      Storm.source(TraversableSpout(original)),
      store
    )

    implicit val s = Storm.local(Map(
      "DEFAULT" -> Options().set(CacheSize(4))
        .set(FlushFrequency(Duration.fromMilliseconds(1)))
    ))

    apply(job)
    TestStore[K, V](id).getOrElse(sys.error("Error running test, unable to find store at the end"))
  }

  def apply(plannedTopology: PlannedTopology) {
    this.synchronized {
      try {
        tryRun(plannedTopology)
      } catch {
        case _: Throwable =>
          Thread.sleep(3000)
          tryRun(plannedTopology)
      }
    }
  }
}
