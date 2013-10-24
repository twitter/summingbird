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

package com.twitter.summingbird.storm

import backtype.storm.Config
import com.twitter.scalding.Args
import com.twitter.chill.{ScalaKryoInstantiator, Kryo, toRich, IKryoRegistrar}
import com.twitter.chill.config.{ ConfiguredInstantiator => ConfInst, JavaMapConfig }
import com.twitter.chill.java.IterableRegistrar
import com.twitter.summingbird.{ Env, Unzip2, Producer, TailProducer }
import com.twitter.summingbird.batch.{ BatchID, Timestamp }
import com.twitter.summingbird.scalding.Scalding
import scala.collection.JavaConverters._

/**
 * Storm-specific extension to Env. StormEnv handles storm-specific configuration
 * and topology submission to the Storm cluster.
 *
 * @author Oscar Boykin
 * @author Sam Ritchie
 * @author Ashu Singhal
 */

case class StormEnv(override val jobName: String, override val args: Args)
    extends Env(jobName) {
  def run {
    // Calling abstractJob's constructor and binding it to a variable
    // forces any side effects caused by that constructor (building up
    // of the environment and defining the builder).
    val ajob = abstractJob

    val classSuffix =
      args.optional("name")
        .getOrElse(jobName.split("\\.").last)

    Storm.remote(builder.opts)
      .withConfigUpdater { config =>
      val c = ConfigBijection.invert(config)
      val transformed = ConfigBijection(ajob.transformConfig(c))
      val kryoConfig = new JavaMapConfig(transformed)
      ConfInst.setSerialized(
        kryoConfig,
        classOf[ScalaKryoInstantiator],
        new ScalaKryoInstantiator()
          .withRegistrar(builder.registrar)
          .withRegistrar(new IterableRegistrar(ajob.registrars))
          .withRegistrar(new IKryoRegistrar {
            def apply(k: Kryo) {
              List(classOf[BatchID], classOf[Timestamp])
                .foreach { cls =>
                  if(!k.alreadyRegistered(cls)) k.register(cls)
                }
            }
          })
      )
      transformed
    }.run(
      builder.node.name(builder.id).asInstanceOf[TailProducer[Storm, _]],
      classSuffix
    )
  }
}
