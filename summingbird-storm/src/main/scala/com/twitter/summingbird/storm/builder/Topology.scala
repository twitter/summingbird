package com.twitter.summingbird.storm.builder

import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.Incrementor
import com.twitter.summingbird.online.executor.{ InputState, KeyValueShards, OperationContainer }
import com.twitter.summingbird.online.option.{ MaxEmitPerExecute, SummerBuilder }
import com.twitter.summingbird.option.JobId
import com.twitter.summingbird.storm.StormMetric
import com.twitter.summingbird.storm.option.{ AckOnEntry, AnchorTuples, MaxExecutePerSecond }
import com.twitter.tormenta.spout.{ Spout => TormentaSpout }
import org.apache.storm.generated.StormTopology
import org.apache.storm.topology.{ IRichBolt, TopologyBuilder }
import org.apache.storm.{ Config => BacktypeStormConfig }
import org.apache.storm.tuple.Tuple
import scala.collection.{ Map => CMap }
import scala.util.Try

private[summingbird] case class Topology(
  spouts: Map[Topology.SpoutId[_], Topology.Spout[_]],
  bolts: Map[Topology.BoltId[_, _], Topology.Bolt[_, _]],
  edges: List[Topology.Edge[_, _]]
) {
  def withSpout[O](id: String, spout: Topology.Spout[O]): (Topology.SpoutId[O], Topology) = {
    val spoutId = Topology.SpoutId[O](id)
    assert(!spouts.contains(spoutId))
    (spoutId, Topology(spouts.updated(spoutId, spout), bolts, edges))
  }

  def withBolt[I, O](id: String, bolt: Topology.Bolt[I, O]): (Topology.BoltId[I, O], Topology) = {
    val boltId = Topology.BoltId[I, O](id)
    assert(!bolts.contains(boltId))
    (boltId, Topology(spouts, bolts.updated(boltId, bolt), edges))
  }

  def withComponent[O](id: String, component: Topology.Component[O]): (Topology.EmittingId[O], Topology) = {
    component match {
      case bolt: Topology.Bolt[_, O] => withBolt(id, bolt)
      case spout: Topology.Spout[O] => withSpout(id, spout)
    }
  }

  def withEdge[I, O](edge: Topology.Edge[I, O]): Topology = {
    assert(edge.source != edge.dest)
    assert(edges.forall { !edge.sameEndPoints(_) })

    Topology(spouts, bolts, edges :+ edge)
  }

  def contains(id: Topology.ComponentId): Boolean = id match {
    case spoutId: Topology.SpoutId[_] => spouts.contains(spoutId)
    case boltId: Topology.BoltId[_, _] => bolts.contains(boltId)
  }

  def incomingEdges[O](id: Topology.ReceivingId[O]): List[Topology.Edge[_, O]] = {
    assert(contains(id))
    edges.filter(_.dest == id).asInstanceOf[List[Topology.Edge[_, O]]]
  }

  def outgoingEdges[I](id: Topology.EmittingId[I]): List[Topology.Edge[I, _]] = {
    assert(contains(id))
    edges.filter(_.source == id).asInstanceOf[List[Topology.Edge[I, _]]]
  }

  def build(jobId: JobId): StormTopology = {
    edges.foreach(edge => assert(contains(edge.source) && contains(edge.dest)))

    val builder = new TopologyBuilder

    spouts.foreach { case (spoutId: Topology.SpoutId[Any], spout: Topology.Spout[Any]) =>
      val builtSpout = SpoutBuilder.build[Any](
        jobId,
        spoutId,
        spout,
        outgoingEdges(spoutId)
      )
      builder.setSpout(spoutId.id, builtSpout, spout.parallelism)
    }

    bolts.foreach { case (boltId: Topology.BoltId[Any, Any], bolt: Topology.Bolt[Any, Any]) =>
      val builtBolt: IRichBolt = BaseBolt[Any, Any](
        jobId,
        boltId,
        bolt.metrics,
        bolt.anchorTuples,
        bolt.ackOnEntry,
        bolt.maxExecutePerSec,
        incomingEdges(boltId).toVector,
        outgoingEdges(boltId).toVector,
        bolt.executor
      )

      val declarer = builder.setBolt(
        boltId.id,
        builtBolt,
        bolt.parallelism
      ).addConfigurations(tickConfig)

      incomingEdges(boltId).foreach { edge =>
        edge.grouping.register(declarer, edge.source)
      }
    }

    builder.createTopology()
  }

  /**
   * Set storm to tick our nodes every second to clean up finished futures
   */
  private def tickConfig = {
    val boltConfig = new BacktypeStormConfig
    boltConfig.put(BacktypeStormConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS, java.lang.Integer.valueOf(1))
    boltConfig
  }
}

private[summingbird] object Topology {
  val empty = Topology(Map(), Map(), List())

  /**
   * Represents id of topology's component.
   */
  sealed trait ComponentId {
    def id: String
  }

  /**
   * Represents id of topology's component which receives tuples.
   * @tparam I represents type of received tuples by this component.
   */
  trait ReceivingId[-I] extends ComponentId

  /**
   * Represents id of topology's component which emits tuples.
   * @tparam O represents type of emitted tuples by this component.
   */
  trait EmittingId[+O] extends ComponentId

  /**
   * Represents id of topology's spout.
   * @tparam O represents type of emitted tuples by this spout.
   */
  case class SpoutId[+O](override val id: String) extends EmittingId[O]

  /**
   * Represents id of topology's bolt.
   * @tparam I represents type on received tuples by this bolt.
   * @tparam O represents type of emitted tuples by this bolt.
   */
  case class BoltId[-I, +O](override val id: String) extends ReceivingId[I] with EmittingId[O]

  /**
   * Represents topology's edge which links two components.
   * @param source id of source component.
   * @param format of tuples going through this edge.
   * @param grouping of tuples going through this edge, should be consistent with format.
   * @param onReceiveTransform will be applied on receiving side for each tuple.
   * @param dest id of destination component.
   * @tparam I type of tuples emitted by source.
   * @tparam O type of tuples received by destination.
   */
  case class Edge[I, O](
    source: EmittingId[I],
    format: OutputFormat[I],
    grouping: EdgeGrouping,
    onReceiveTransform: I => O,
    dest: ReceivingId[O]
  ) {
    def sameEndPoints(another: Edge[_, _]): Boolean =
      source == another.source && dest == another.dest

    def deserialize(serialized: java.util.List[AnyRef]): Try[O] =
      format.injection.invert(serialized).map(onReceiveTransform)
  }

  /**
   * Base trait for all components with parallelism and metrics.
   * @tparam O represent type of values emitted by this component.
   */
  sealed trait Component[+O] {
    def parallelism: Int
    def metrics: () => TraversableOnce[StormMetric[_]]
  }

  /**
   * Base trait for spouts.
   * There are two implementations: raw tormenta spout and key value spout.
   */
  sealed trait Spout[+O] extends Component[O]

  case class RawSpout[+O](
    parallelism: Int,
    metrics: () => TraversableOnce[StormMetric[_]],
    spout: TormentaSpout[O]
  ) extends Spout[O]

  case class KeyValueSpout[K, V: Semigroup](
    parallelism: Int,
    metrics: () => TraversableOnce[StormMetric[_]],
    spout: TormentaSpout[(K, V)],
    shards: KeyValueShards,
    summerBuilder: SummerBuilder,
    maxEmitPerExec: MaxEmitPerExecute,
    flushExecTimeCounter: Incrementor,
    executeTimeCounter: Incrementor
  ) extends Spout[(Int, CMap[K, V])] {
    val semigroup: Semigroup[V] = implicitly[Semigroup[V]]
  }

  /**
   * Base class for bolts.
   */
  case class Bolt[-I, +O](
    parallelism: Int,
    metrics: () => TraversableOnce[StormMetric[_]],
    anchorTuples: AnchorTuples,
    ackOnEntry: AckOnEntry,
    maxExecutePerSec: MaxExecutePerSecond,
    executor: OperationContainer[I, O, InputState[Tuple]]
  ) extends Component[O]
}
