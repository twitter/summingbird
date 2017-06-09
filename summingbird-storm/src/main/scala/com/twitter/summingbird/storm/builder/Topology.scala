package com.twitter.summingbird.storm.builder

import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.Incrementor
import com.twitter.summingbird.online.executor.{ InputState, OperationContainer }
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

private[summingbird] case class Topology(
  spouts: Map[Topology.SpoutId[_], Topology.Spout[_]],
  bolts: Map[Topology.BoltId[_, _], Topology.Bolt[_, _]],
  edges: List[Topology.Edge[_]]
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

  def withEdge[T](edge: Topology.Edge[T]): Topology = {
    assert(contains(edge.source) && contains(edge.dest))
    assert(edge.source != edge.dest)
    assert(edges.forall { anotherEdge =>
      !(anotherEdge.source == edge.source && anotherEdge.dest == edge.dest)
    })

    Topology(spouts, bolts, edges :+ edge)
  }

  def contains(id: Topology.ComponentId): Boolean = id match {
    case spoutId: Topology.SpoutId[_] => spouts.contains(spoutId)
    case boltId: Topology.BoltId[_, _] => bolts.contains(boltId)
  }

  def incomingEdges[T](id: Topology.ReceivingId[T]): List[Topology.Edge[T]] = {
    assert(contains(id))
    edges.filter(_.dest == id).asInstanceOf[List[Topology.Edge[T]]]
  }

  def outgoingEdges[T](id: Topology.EmittingId[T]): List[Topology.Edge[T]] = {
    assert(contains(id))
    edges.filter(_.source == id).asInstanceOf[List[Topology.Edge[T]]]
  }

  def build(jobId: JobId): StormTopology = {
    val builder = new TopologyBuilder

    spouts.foreach { case (spoutId: Topology.SpoutId[Any], spout: Topology.Spout[Any]) =>
      val builtSpout = SpoutBuilder[Any](
        jobId,
        spoutId,
        spout,
        outgoingEdges(spoutId)
      )
      builder.setSpout(spoutId.id, builtSpout, spout.parallelism)
    }

    bolts.foreach { case (boltId: Topology.BoltId[Any, Any], bolt: Topology.Bolt[Any, Any]) =>
      val builtBolt: IRichBolt = BoltBuilder.apply[Any, Any](
        jobId,
        boltId,
        bolt,
        incomingEdges(boltId),
        outgoingEdges(boltId)
      )

      val declarer = builder.setBolt(
        boltId.id,
        builtBolt,
        bolt.parallelism
      ).addConfigurations(tickConfig)

      incomingEdges(boltId).foreach { edge =>
        edge.edgeType.grouping.apply(declarer, edge.source)
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
  val EMPTY = Topology(Map(), Map(), List())

  sealed trait ComponentId {
    val id: String
  }

  trait ReceivingId[-I] extends ComponentId

  trait EmittingId[+O] extends ComponentId

  case class SpoutId[+O](override val id: String) extends EmittingId[O]

  case class BoltId[-I, +O](override val id: String) extends ReceivingId[I] with EmittingId[O]

  case class Edge[T](source: EmittingId[T], edgeType: EdgeType[T], dest: ReceivingId[T])

  sealed trait Component {
    val parallelism: Int
    val metrics: () => TraversableOnce[StormMetric[_]]
  }

  trait Spout[+O] extends Component

  case class RawSpout[+O](
    override val parallelism: Int,
    override val metrics: () => TraversableOnce[StormMetric[_]],
    spout: TormentaSpout[O]
  ) extends Spout[O]

  case class KeyValueSpout[K, V: Semigroup](
    override val parallelism: Int,
    override val metrics: () => TraversableOnce[StormMetric[_]],
    spout: TormentaSpout[(K, V)],
    summerBuilder: SummerBuilder,
    maxEmitPerExec: MaxEmitPerExecute,
    flushExecTimeCounter: Incrementor,
    executeTimeCounter: Incrementor
  ) extends Spout[(Int, CMap[K, V])] {
    val semigroup: Semigroup[V] = implicitly[Semigroup[V]]
  }

  case class Bolt[-I, +O](
    override val parallelism: Int,
    override val metrics: () => TraversableOnce[StormMetric[_]],
    anchorTuples: AnchorTuples,
    ackOnEntry: AckOnEntry,
    maxExecutePerSec: MaxExecutePerSecond,
    executor: OperationContainer[I, O, InputState[Tuple]]
  ) extends Component
}
