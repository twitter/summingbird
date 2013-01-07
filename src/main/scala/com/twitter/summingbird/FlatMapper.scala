package com.twitter.summingbird

import com.twitter.chill.ClosureCleaner
import com.twitter.summingbird.util.CacheSize

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

/**
 * Main class to represents a realtime flatmap operation
 * Event: the type of real time object you are flatmapping
 * Key: the type you are sharding on.
 * Value: the ultimate type for the aggregation.
 */

// Summingbird passes each Event produced by the
// EventSource[Event,Time] into a user-supplied
// FlatMapper[Event,Key,Value] function that implements the following
// trait:

trait FlatMapper[Event,Key,Value] extends java.io.Serializable {
  // transform an event to key value pairs
  def encode(thisEvent: Event): TraversableOnce[(Key,Value)]
  def cleanup: Unit
  def prepare(config: java.util.Map[_,_]): Unit
}

// For aggregations that don't require prepare or cleanup
// implementations, FunctionFlatMapper allows the user to provide a
// flatmapping Function1 to the Summingbird DSL directly.

class FunctionFlatMapper[Event,Key,Value](fn: (Event) => TraversableOnce[(Key,Value)])
extends FlatMapper[Event,Key,Value] {

  // Necessary to remove the $outer reference from the captured
  // function. Without this, Kryo's FieldSerializer tries to serialize
  // the $outer variable and pulls in the entire surrounding scope.
  ClosureCleaner(fn)

  override def encode(thisEvent : Event) = fn(thisEvent)
  override def cleanup { }
  override def prepare(conf : java.util.Map[_,_]) { }
}

// Implicit conversion to facilitate the use of Function1 described
// above.

object FlatMapper {
  implicit def functionToFlatMapper[Event,Key,Value](fn : (Event) => TraversableOnce[(Key,Value)])
  : FlatMapper[Event,Key,Value] = new FunctionFlatMapper(fn)
}
