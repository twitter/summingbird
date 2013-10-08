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

import Constants._
import com.twitter.algebird.Monoid
import com.twitter.chill.ScalaKryoInstantiator
import com.twitter.chill.config.{ ConfiguredInstantiator, JavaMapConfig }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.algebra.MergeableStore.enrich
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.akka.option.CacheSize
import com.twitter.summingbird.planner._
import com.twitter.summingbird.akka.planner._
import com.twitter.util.Future
import scala.annotation.tailrec
import _root_.akka.actor.ActorSystem
import _root_.akka.routing.{RandomRouter, ConsistentHashingRouter}
import _root_.akka.actor.Props
import com.twitter.chill.MeatLocker
import _root_.akka.routing.ConsistentHashingRouter

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
  type Source[+T] = AkkaSource[T]
  type Store[-K, V] = AkkaStore[K, V]
  type Sink[-T] = () => (T => Future[Unit])
  type Service[-K, +V] = AkkaService[K, V]
  type Plan[T] = List[(Props, String)]

  private type Prod[T] = Producer[Akka, T]
  
  private def cleanName(name: String): String = {
    name.replaceAll("""[ \[\]\(\)-]""","_")
  }

  private def getOrElse[T: Manifest](node: AkkaNode, default: T): T = {
    val producer = node.members.last
    
    val namedNodes = Dependants(producer).transitiveDependantsOf(producer).collect{case NamedProducer(_, n) => n}
    (for {
      id <- namedNodes
      akkaOpts <- options.get(id)
      option <- akkaOpts.get[T]
    } yield option).headOption.getOrElse(default)
  }

  protected def scheduleFlatMapper(dag: Dag[Akka], node: AkkaNode): (Props, String) = {
    /**
     * Only exists because of the crazy casts we needed.
     */
    def foldOperations(producers: List[Producer[Akka, _]]): FlatMapOperation[Any, Any] = {
      producers.foldLeft(FlatMapOperation.identity[Any]) {
        case (acc, p) =>
          p match {
            case LeftJoinedProducer(_, StoreWrapper(newService)) =>
              FlatMapOperation.combine(
                acc.asInstanceOf[FlatMapOperation[Any, (Any, Any)]],
                newService.asInstanceOf[StoreFactory[Any, Any]]).asInstanceOf[FlatMapOperation[Any, Any]]
            case OptionMappedProducer(_, op) => acc.andThen(FlatMapOperation[Any, Any](op.andThen(_.iterator).asInstanceOf[Any => TraversableOnce[Any]]))
            case FlatMappedProducer(_, op) => acc.andThen(FlatMapOperation(op).asInstanceOf[FlatMapOperation[Any, Any]])
            case WrittenProducer(_, sinkSupplier) => acc.andThen(FlatMapOperation.write(sinkSupplier.asInstanceOf[() => (Any => Future[Unit])]))
            case IdentityKeyedProducer(_) => acc
            case NamedProducer(_, _) => acc
            case _ => throw new Exception("Not found! : " + p)
          }
      }
    }
    val nodeName = cleanName(dag.getNodeName(node))
    val operation = foldOperations(node.members.reverse)
    val targetNames: List[String] = dag.dependantsOf(node).collect{ case x: AkkaNode => cleanName(dag.getNodeName(x)) }
    
    (Props(new FlatMapActor(operation, targetNames)).withRouter(RandomRouter(1)), nodeName)
  }

  protected def buildSourceProps[K](dag: Dag[Akka], node: AkkaNode): (Props, String) = {
    val source = node.members.collect { case Source(s) => s }.head
    val nodeName = cleanName(dag.getNodeName(node))

    val src = node.members.reverse.foldLeft(source.asInstanceOf[AkkaSource[Any]]) {
      case (spout, Source(_)) => spout // The source is still in the members list so drop it
      case (spout, OptionMappedProducer(_, op)) => spout.flatMap{x => op.apply(x)}
      case (spout, NamedProducer(_, _)) => spout
      case _ => sys.error("not possible, given the above call to span.")
    }
     val targetNames: List[String] = dag.dependantsOf(node).collect{ case x: AkkaNode => cleanName(dag.getNodeName(x)) }

    (Props(new SourceActor(src, targetNames)).withRouter(RandomRouter(1)), nodeName)
  }

  private def scheduleSummer[K, V](dag: Dag[Akka], node: AkkaNode): (Props, String) = {
    val summer: Summer[Akka, K, V] = node.members.collect { case c: Summer[Akka, K, V] => c }.head
    implicit val monoid = summer.monoid
    val nodeName = cleanName(dag.getNodeName(node))

    val supplier = summer.store match {
      case MergeableStoreSupplier(contained, _) => contained
    }
    implicit val batcher = summer.store.batcher
    
    (Props(new SummerActor(supplier)).withRouter(ConsistentHashingRouter(1)), nodeName)
  }

  /**
   * The following operations are public.
   */

  def plan[T](tail: Producer[Akka, T]): List[(Props, String)] = {
     val dag = OnlinePlan(tail)
     dag.nodes.map{ node =>
     	node match {
         case s: SummerNode[_] => scheduleSummer(dag, s)
         case f: FlatMapNode[_] => scheduleFlatMapper(dag, f)
         case s: SourceNode[_] => buildSourceProps(dag, s)
       }
     }
  }
  def run(tail: Producer[Akka, _], jobName: String): Unit = run(plan(tail), jobName)
  def run(topology: Plan[Akka], jobName: String): Unit
}

class LocalAkka(options: Map[String, Options])
  extends Akka(options) {
  lazy val localCluster = ActorSystem("localSummingBird")

  def shutdown = localCluster.shutdown
  def awaitTermination = localCluster.awaitTermination
  override def run(plan: List[(Props, String)], jobName: String): Unit = {
    plan.foreach { case (p, name) =>
      localCluster.actorOf(p, name = name)
    }
  }
}
