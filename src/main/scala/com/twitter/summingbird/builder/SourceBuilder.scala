package com.twitter.summingbird.builder

import com.twitter.algebird.Monoid
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.source.EventSource
import com.twitter.summingbird.{ FlatMapper, FunctionFlatMapper }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

// The SourceBuilder is the first level of the expansion.

class SourceBuilder[Event: Manifest, Time : Batcher: Manifest]
(eventSource: EventSource[Event,Time], val predOption: Option[(Event) => Boolean] = None)
extends java.io.Serializable {

  def filter(newPred: (Event) => Boolean): SourceBuilder[Event,Time] = {
    val newPredicate =
      predOption.map { old => { (e: Event) => old(e) && newPred(e) } }
        .orElse(Some(newPred))

    new SourceBuilder(eventSource, newPredicate)
  }

  def map[Key: Manifest: Ordering,Val: Manifest: Monoid](fn: (Event) => (Key,Val)) =
    flatMap {e : Event => Some(fn(e)) }

  def flatMap[Key: Manifest: Ordering,Val: Manifest: Monoid]
  (fn : (Event) => TraversableOnce[(Key,Val)])  =
    flatMapBuilder(new FunctionFlatMapper(fn))

  def flatMapBuilder[Key,Val](newFlatMapper: FlatMapper[Event,Key,Val])
  (implicit keyOrdering: Ordering[Key], kmf: Manifest[Key], vmf: Manifest[Val], monoid: Monoid[Val])
  : SingleFlatMappedBuilder[Event,Time,Key,Val] =
    new SingleFlatMappedBuilder[Event,Time,Key,Val](eventSource, keyOrdering, predOption, newFlatMapper)
}
