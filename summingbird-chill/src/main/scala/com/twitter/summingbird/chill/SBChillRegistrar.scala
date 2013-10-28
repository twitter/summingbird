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

package com.twitter.summingbird.chill
import com.twitter.summingbird.{MutableStringConfig, SummingbirdConfig}
import com.twitter.chill.{ScalaKryoInstantiator, IKryoRegistrar, Kryo, toRich}
import com.twitter.chill.java.IterableRegistrar
import com.twitter.chill.config.{ ConfiguredInstantiator => ConfInst }
import com.twitter.summingbird.batch.{BatchID, Timestamp}

object SBChillRegistrar {
  def apply(cfg: SummingbirdConfig, iterableRegistrars: List[IKryoRegistrar]): SummingbirdConfig = {
    val kryoConfig = new com.twitter.chill.config.Config with MutableStringConfig {
      val summingbirdConfig = cfg
    }

    ConfInst.setSerialized(
      kryoConfig,
      classOf[ScalaKryoInstantiator],
      new ScalaKryoInstantiator()
        .withRegistrar(new IterableRegistrar(iterableRegistrars))
        .withRegistrar(new IKryoRegistrar {
          def apply(k: Kryo) {
            List(classOf[BatchID], classOf[Timestamp])
              .foreach { cls =>
                if(!k.alreadyRegistered(cls)) k.register(cls)
              }
          }
        })
    )
    kryoConfig.unwrap
  }
}
