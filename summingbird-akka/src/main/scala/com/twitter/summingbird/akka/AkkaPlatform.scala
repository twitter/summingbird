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

package com.twitter.summingbird.akka

import com.twitter.algebird.Monoid
import com.twitter.chill.ScalaKryoInstantiator
import com.twitter.chill.config.{ ConfiguredInstantiator, JavaMapConfig }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird._
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.planner._
import com.twitter.util.Future
import com.twitter.summingbird.online.executor
import com.twitter.summingbird.online.FlatMapOperation
import com.twitter.summingbird.online.option.{IncludeSuccessHandler, FlushFrequency}
import com.twitter.summingbird.batch.{BatchID, Timestamp}


import scala.annotation.tailrec
import _root_.akka.actor.ActorSystem
import _root_.akka.routing.{RandomRouter, ConsistentHashingRouter}
import _root_.akka.actor.Props
import com.twitter.chill.MeatLocker
import _root_.akka.routing.ConsistentHashingRouter
import org.slf4j.LoggerFactory


sealed trait AkkaStore[-K, V] {
  def batcher: Batcher
}

object MergeableStoreSupplier {
  def from[K, V](store: => MergeableStore[(K, BatchID), V])(implicit batcher: Batcher): MergeableStoreSupplier[K, V] =
    MergeableStoreSupplier(() => store, batcher)
}

case class MergeableStoreSupplier[K, V](store: () => MergeableStore[(K, BatchID), V], batcher: Batcher) extends AkkaStore[K, V]

sealed trait AkkaService[-K, +V]
case class StoreWrapper[K, V](store: StoreFactory[K, V]) extends AkkaService[K, V]

object Akka {
  def local(options: Map[String, Options] = Map.empty): LocalAkka =
    new LocalAkka(options)

  def store[K, V](store: => MergeableStore[(K, BatchID), V])(implicit batcher: Batcher): MergeableStoreSupplier[K, V] =
    MergeableStoreSupplier.from(store)

  implicit def source[T: Manifest](spout: AkkaSource[T]) =
    Producer.source[Akka, T](spout)
}

abstract class Akka(options: Map[String, Options]) extends Platform[Akka] {
  import Constants._
  @transient private val logger = LoggerFactory.getLogger(classOf[Akka])

  type Source[+T] = AkkaSource[T]
  type Store[-K, V] = AkkaStore[K, V]
  type Sink[-T] = () => (T => Future[Unit])
  type Service[-K, +V] = AkkaService[K, V]
  type Plan[T] = List[(Props, String)]
  type AkkaNode = Node[Akka]

  private type Prod[T] = Producer[Akka, T]

  private def cleanName(name: String): String = {
    name.replaceAll("""[\| \[\]\(\)-]""","_")
  }


  private def getOrElse[T <: AnyRef : Manifest](dag: Dag[Akka], node: AkkaNode, default: T): T = {
    val producer = node.members.last

    val namedNodes = dag.producerToPriorityNames(producer)
    val maybePair = (for {
      id <- namedNodes :+ "DEFAULT"
      stormOpts <- options.get(id)
      option <- stormOpts.get[T]
    } yield (id, option)).headOption

    maybePair match {
      case None =>
          logger.debug("Node ({}): Using default setting {}", dag.getNodeName(node), default)
          default
      case Some((namedSource, option)) =>
          logger.info("Node {}: Using {} found via NamedProducer \"{}\"", Array[AnyRef](dag.getNodeName(node), option, namedSource))
          option
    }
  }



  protected def scheduleFlatMapper(dag: Dag[Akka], node: AkkaNode): (Props, String) = {
    /**
     * Only exists because of the crazy casts we needed.
     */
    def foldOperations(producers: List[Producer[Akka, _]]): FlatMapOperation[Any, Any] = {
      producers.foldLeft(FlatMapOperation.identity[Any]) {
        case (acc, p) =>
          p match {
            case LeftJoinedProducer(_, wrapper) =>
              val newService = wrapper.asInstanceOf[StoreWrapper[Any, Any]].store
              FlatMapOperation.combine(
                acc.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
                newService.asInstanceOf[StoreFactory[Any, Any]]).asInstanceOf[FlatMapOperation[Any, Any]]
            case OptionMappedProducer(_, op) => acc.andThen(FlatMapOperation[Any, Any](op.andThen(_.iterator).asInstanceOf[Any => TraversableOnce[Any]]))
            case FlatMappedProducer(_, op) => acc.andThen(FlatMapOperation(op).asInstanceOf[FlatMapOperation[Any, Any]])
            case WrittenProducer(_, sinkSupplier) => acc.andThen(FlatMapOperation.write(sinkSupplier.asInstanceOf[() => (Any => Future[Unit])]))
            case IdentityKeyedProducer(_) => acc
            case NamedProducer(_, _) => acc
            case AlsoProducer(_, _) => acc
            case KeyFlatMappedProducer(_, op) => acc.andThen(FlatMapOperation.keyFlatMap[Any, Any, Any](op).asInstanceOf[FlatMapOperation[Any, Any]])
            case MergedProducer(_, _) => acc
            case Source(_) => sys.error("A source should never be in a flatmapper")
            case Summer(_, _, _) => sys.error("A summer should never be in a flatmapper")
          }
      }
    }
    val nodeName = cleanName(dag.getNodeName(node))
    val operation = foldOperations(node.members.reverse)
    val maxWaiting = getOrElse(dag, node, DEFAULT_MAX_WAITING_FUTURES)
    val maxWaitTime = getOrElse(dag, node, DEFAULT_MAX_FUTURE_WAIT_TIME)
    logger.info("[{}] maxWaiting: {}", nodeName, maxWaiting.get)

    val flushFrequency = getOrElse(dag, node, DEFAULT_FLUSH_FREQUENCY)
    logger.info("[{}] maxWaiting: {}", nodeName, flushFrequency.get)



    val targetNames: List[String] = dag.dependantsOf(node).collect{ case x: AkkaNode => cleanName(dag.getNodeName(x)) }

    val summerOpt:Option[SummerNode[Akka]] = dag.dependantsOf(node).collect{case s: SummerNode[Akka] => s}.headOption

    val builtActor = summerOpt match {
      case Some(s) =>
      val summerProducer = s.members.collect { case s: Summer[_, _, _] => s }.head.asInstanceOf[Summer[Akka, _, _]]

        Props(BaseActor(
          targetNames,
          true,
          new executor.FinalFlatMap(
            operation.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
            getOrElse(dag, node, DEFAULT_FM_CACHE),
            flushFrequency,
            maxWaiting,
            maxWaitTime,
            new SingleItemInjection[Any],
            new KeyValueInjection[(Any, BatchID), Any]
            )(summerProducer.monoid.asInstanceOf[Monoid[Any]], summerProducer.store.batcher)
            ))
      case None =>
       Props(BaseActor(
          targetNames,
          dag.dependantsOf(node).size > 0,
          new executor.IntermediateFlatMap(
            operation,
            maxWaiting,
            maxWaitTime,
            new SingleItemInjection[Any],
            new SingleItemInjection[Any]
            )
          ))
    }
    (builtActor.withRouter(RandomRouter(1)), nodeName)
  }

  protected def buildSourceProps[K](dag: Dag[Akka], node: AkkaNode): (Props, String) = {
    val source = node.members.collect { case Source(s) => s }.head
    val nodeName = cleanName(dag.getNodeName(node))

    val src = node.members.reverse.foldLeft(source.asInstanceOf[AkkaSource[Any]]) { case (spout, producer) =>
      producer match {
        case Source(_) => spout // The source is still in the members list so drop it
        case OptionMappedProducer(_, op) => spout.flatMap{x => op.apply(x)}
        case NamedProducer(_, _) => spout
        case AlsoProducer(_, _) => spout
        case FlatMappedProducer(_, _) => sys.error("Should never include a FlatMapper in a source")
        case IdentityKeyedProducer(_) => spout
        case KeyFlatMappedProducer(_, _) => sys.error("Should never include a Key Flat Mapper in a source")
        case LeftJoinedProducer(_, _) => sys.error("Should never include a LeftJoinedProducer in a source")
        case MergedProducer(_, _) => sys.error("Should never include a Merged Producer in a source")
        case Summer(_, _, _) => sys.error("Should never include a Summer in a source")
        case WrittenProducer(_, _) => sys.error("Should never include a WrittenProducer in a source")
      }
    }

     val targetNames: List[String] = dag.dependantsOf(node).collect{ case x: AkkaNode => cleanName(dag.getNodeName(x)) }


    (Props(new SourceActor(src, targetNames, new SingleItemInjection[Any])).withRouter(RandomRouter(1)), nodeName)
  }

  private def scheduleSummer[K, V](dag: Dag[Akka], node: AkkaNode): (Props, String) = {
    val summer: Summer[Akka, K, V] = node.members.collect { case c: Summer[Akka, K, V] => c }.head
    implicit val monoid = summer.monoid
    val nodeName = cleanName(dag.getNodeName(node))

    val supplier = summer.store match {
      case MergeableStoreSupplier(contained, _) => contained
    }
    implicit val batcher = summer.store.batcher

    val shouldEmit = dag.dependantsOf(node).size > 0
    val targetNames: List[String] = dag.dependantsOf(node).collect{ case x: AkkaNode => cleanName(dag.getNodeName(x)) }


    val summerActor = Props(BaseActor(
          targetNames,
          shouldEmit,
          new executor.Summer(
              supplier,
              getOrElse(dag, node, DEFAULT_ONLINE_SUCCESS_HANDLER),
              getOrElse(dag, node, DEFAULT_ONLINE_EXCEPTION_HANDLER),
              getOrElse(dag, node, DEFAULT_SUMMER_CACHE),
              getOrElse(dag, node, DEFAULT_MAX_WAITING_FUTURES),
              getOrElse(dag, node, DEFAULT_MAX_FUTURE_WAIT_TIME),
              getOrElse(dag, node, IncludeSuccessHandler.default),
              new KeyValueInjection[(K,BatchID), V],
              new SingleItemInjection[(K, (Option[V], V))])
        ))
    (summerActor.withRouter(ConsistentHashingRouter(1)), nodeName)
  }

  /**
   * The following operations are public.
   */

  def plan[T](tail: TailProducer[Akka, T]): List[(Props, String)] = {
     val dag = OnlinePlan(tail)
     dag.nodes.map{ node =>
     	node match {
         case s: SummerNode[_] => scheduleSummer(dag, s)
         case f: FlatMapNode[_] => scheduleFlatMapper(dag, f)
         case s: SourceNode[_] => buildSourceProps(dag, s)
       }
     }
  }
  def run(tail: TailProducer[Akka, _], jobName: String): Unit = run(plan(tail), jobName)
  def run(topology: Plan[Akka], jobName: String): Unit
}

class LocalAkka(options: Map[String, Options])
  extends Akka(options) {
  lazy val localCluster = ActorSystem("localSummingBird" + scala.util.Random.alphanumeric.take(5).mkString(""))

  def shutdown = localCluster.shutdown
  def awaitTermination = localCluster.awaitTermination
  override def run(plan: List[(Props, String)], jobName: String): Unit = {
    plan.foreach { case (p, name) =>
      Future {
        localCluster.actorOf(p, name = name)
      }
    }
  }
}
