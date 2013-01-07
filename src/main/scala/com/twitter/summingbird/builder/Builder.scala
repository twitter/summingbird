package com.twitter.summingbird.builder

import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.source.{ EventSource, OfflineSource, OnlineSource }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

trait OnlineSourceImplicits {
  implicit def onlineSourceToBuilder[Event: Manifest,Time : Batcher: Manifest](es: OnlineSource[Event,Time])
  : SourceBuilder[Event,Time] =
    new SourceBuilder[Event,Time](EventSource.onlineToEvent(es))
}

trait OfflineSourceImplicits extends OnlineSourceImplicits {
  implicit def offlineSourceToBuilder[Event: Manifest,Time:Batcher : Manifest](es: OfflineSource[Event,Time])
  : SourceBuilder[Event,Time] =
    new SourceBuilder[Event,Time](EventSource.offlineToEvent(es))
}

object Builder extends OfflineSourceImplicits {
  // Allows us to call .flatMap on EventSource to start the builder process
  implicit def sourceToBuilder[Event: Manifest,Time: Batcher : Manifest](es: EventSource[Event,Time])
  : SourceBuilder[Event,Time] =
    new SourceBuilder[Event,Time](es)
}
