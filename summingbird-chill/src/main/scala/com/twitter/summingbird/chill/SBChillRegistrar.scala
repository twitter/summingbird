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
import com.twitter.summingbird.{ MutableStringConfig, SummingbirdConfig }
import com.twitter.chill.{ ScalaKryoInstantiator, IKryoRegistrar, Kryo, toRich, ReflectingRegistrar, InjectionDefaultRegistrar, InjectionRegistrar }
import com.twitter.chill.java.IterableRegistrar
import com.twitter.bijection.Codec
import com.twitter.chill._
import com.twitter.chill.config.{ ConfiguredInstantiator => ConfInst }
import com.twitter.summingbird.batch.{ BatchID, Timestamp }

/**
 * @author Oscar Boykin
 * @author Ian O Connell
 */

object SBChillRegistrar {

  def injectionRegistrar[T: Manifest: Codec]: InjectionRegistrar[T] =
    InjectionRegistrar(manifest[T].runtimeClass.asInstanceOf[Class[T]], implicitly[Codec[T]])

  def injectionDefaultRegistrar[T: Manifest: Codec]: InjectionDefaultRegistrar[T] =
    InjectionDefaultRegistrar(manifest[T].runtimeClass.asInstanceOf[Class[T]], implicitly[Codec[T]])

  def kryoRegClass(clazz: Class[_]*) =
    { k: Kryo =>
      clazz
        .filter(k.alreadyRegistered(_))
        .foreach(k.register(_))
    }

  def apply(cfg: SummingbirdConfig, iterableRegistrars: List[IKryoRegistrar]): SummingbirdConfig = {
    val kryoConfig = new com.twitter.chill.config.Config with MutableStringConfig {
      def summingbirdConfig = cfg
    }

    val defaults = List(new ReflectingRegistrar(classOf[BatchID], classOf[BatchIDSerializer]),
      new ReflectingRegistrar(classOf[Timestamp], classOf[TimestampSerializer]))

    ConfInst.setSerialized(
      kryoConfig,
      classOf[ScalaKryoInstantiator],
      new ScalaKryoInstantiator()
        .withRegistrar(new IterableRegistrar(iterableRegistrars ++ defaults))
        .setReferences(false)
    )
    kryoConfig.unwrap
  }
}

