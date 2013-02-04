/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.summingbird.builder

import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.source.{ EventSource, OfflineSource, OnlineSource }

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 */

trait OnlineSourceImplicits {
  implicit def onlineSourceToBuilder[Event: Manifest,Time: Batcher: Manifest](es: OnlineSource[Event,Time])
  : SourceBuilder[Event,Time] =
    new SourceBuilder[Event,Time](EventSource.onlineToEvent(es))
}

trait OfflineSourceImplicits extends OnlineSourceImplicits {
  implicit def offlineSourceToBuilder[Event: Manifest,Time: Batcher: Manifest](es: OfflineSource[Event,Time])
  : SourceBuilder[Event,Time] =
    new SourceBuilder[Event,Time](EventSource.offlineToEvent(es))
}

trait BuilderImplicits extends OfflineSourceImplicits {
  // Allows us to call .flatMap on EventSource to start the builder process
  implicit def sourceToBuilder[Event: Manifest, Time: Manifest: Batcher](es: EventSource[Event, Time])
  : SourceBuilder[Event,Time] =
    new SourceBuilder[Event,Time](es)
}

object Builder extends BuilderImplicits
