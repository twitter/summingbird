package com.twitter.summingbird.memory

import com.twitter.summingbird._
import scala.collection.mutable

object AlexPlatform {
  implicit def toSource[T](traversable: TraversableOnce[T])(implicit mf: Manifest[T]): Producer[AlexPlatform, T] =
    Producer.source[AlexPlatform, T](traversable.toStream)
}

class AlexPlatform extends Platform[AlexPlatform] {

  type Source[T] = Stream[T]
  type Store[K, V] = mutable.Map[K, V]
  type Sink[-T] = (T => Unit)
  type Service[-K, +V] = (K => Option[V])
  type Plan[T] = Stream[T]

  override def plan[T](completed: TailProducer[AlexPlatform, T]): Plan[T] = {
    val (s, _) = toStream(completed, Map.empty)
    s
  }

  type Prod[T] =  Producer[AlexPlatform, T]

  def toStream[T](producer: Prod[T], visited:  Map[Prod[_], Plan[_]]): (Plan[T], Map[Prod[_], Plan[_]]) = {
    // short circuit if we've already visited this node
    visited.get(producer) match {
      case Some(s) => (s.asInstanceOf[Plan[T]], visited)
      case None => toStream2(producer, visited)
    }
  }

  def toStream2[T](outer: Prod[T], visited:  Map[Prod[_], Plan[_]]): (Plan[T], Map[Prod[_], Plan[_]]) = {
    // we've already done the short circuit logic by the time we get here

    val (stream, updatedVisited) = outer match {
      case NamedProducer(prod, _) => toStream(prod, visited)

      case IdentityKeyedProducer(prod) => toStream(prod, visited)

      case Source(source) => (source, visited)

      case OptionMappedProducer(prod, fn) => {
        val (s, v) = toStream(prod, visited)
        (s.flatMap(fn(_)), v)
      }

      case FlatMappedProducer(prod, fn) => {
        val (s, v) = toStream(prod, visited)
        (s.flatMap(fn(_)), v)
      }

      case MergedProducer(l, r) => {
        val (sl, vl) = toStream(l, visited)
        val (sr, vr) = toStream(r, vl)
        (sl ++ sr, vr)
      }

      case KeyFlatMappedProducer(prod, fn) => {
        val (s, v) = toStream(prod, visited)
        val mapped = s.flatMap {
          case (key, value) => fn(key).map { (_, value) }
        }
        (mapped , v)
      }

      case AlsoProducer(ensure, result) => {
        val (ensureStream, ensureVisited) = toStream(ensure, visited)
        ensureStream.force // evaluate the ensure stream
        val (resultStream, resultVisited) = toStream(result, ensureVisited)
        (resultStream, resultVisited)
      }

      case WrittenProducer(prod, fn) => {
        val (s, v) = toStream(prod, visited)
        s.foreach(fn(_))
        (s, v)
      }

      case LeftJoinedProducer(prod, service) => {
        val (s, v) = toStream(prod, visited)
        val joined = s.map { case (key, value) => (key, (value, service(key))) }
        (joined, v)
      }

      case Summer(prod, store, monoid) => {
        val (s, v) = toStream(prod, visited)
        val summed = s.map {
          case kv @ (k, deltaV) => {
            val oldV = store.get(k)
            val newV = oldV.map { x => monoid.plus(x, deltaV) }.getOrElse(deltaV)
            store.update(k, newV)
            (k, (oldV, newV))
          }
        }
        (summed, v)
      }
    }

    (stream.asInstanceOf[Plan[T]], updatedVisited + (outer -> stream))
  }

  def run(iter: Stream[_]) {
    // Force the entire stream, taking care not to hold on to the
    // tail.
    iter.foreach(identity(_: Any))
  }

}
