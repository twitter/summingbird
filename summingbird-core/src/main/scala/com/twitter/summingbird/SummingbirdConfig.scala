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

package com.twitter.summingbird

trait SummingbirdConfig { self =>
  def get(key: String): Option[AnyRef]
  def put(key: String, v: AnyRef): SummingbirdConfig
  final def +(kv: (String, AnyRef)) = put(kv._1, kv._2)
  final def -(k: String) = remove(k)
  def remove(key: String): SummingbirdConfig
  def keys: Iterable[String]
  def updates: Map[String, AnyRef]
  def removes: Set[String]
  def toMap: Map[String, AnyRef] = new Map[String, AnyRef] {
    def get(k: String) = self.get(k)
    def +[B1 >: AnyRef](kv: (String, B1)) = self.put(kv._1, kv._2.asInstanceOf[AnyRef]).toMap
    def -(k: String) = self.-(k).toMap
    def iterator = self.keys.iterator.map(k => (k, self.get(k).get))
  }
  def updated(newMap: Map[String, AnyRef]): SummingbirdConfig = {
    val removedKeys: Set[String] = keys.toSet -- newMap.keys
    val changedOrAddedKeys = newMap.flatMap {
      case (k, v) =>
        val oldVal = get(k)
        if (oldVal != Some(v)) {
          Some((k, v))
        } else None
    }
    val newWithoutRemoved = removedKeys.foldLeft(self)(_.remove(_))
    changedOrAddedKeys.foldLeft(newWithoutRemoved) { _ + _ }
  }
}

trait MutableStringConfig {
  protected def summingbirdConfig: SummingbirdConfig
  private var config = summingbirdConfig
  def get(key: String) = {
    assert(config != null)
    config.get(key) match {
      case Some(s) => s.toString
      case None => null
    }
  }

  def set(key: String, value: String) {
    assert(config != null)
    config = config.put(key, value)
  }
  def unwrap = config
}

/*
 * The ReadableMap is the trait that must be implemented on the actual underlying config for the WrappingConfig.
 * That is one of these should exist for an Hadoop Configuration, Storm Configuration, etc..
 */
trait ReadableMap {
  def get(key: String): Option[AnyRef]
  def keys: Set[String]
}

object WrappingConfig {
  def apply(backingConfig: ReadableMap) = new WrappingConfig(
    backingConfig,
    Map[String, AnyRef](),
    Set[String]())
}

case class WrappingConfig(private val backingConfig: ReadableMap,
    updates: Map[String, AnyRef],
    removes: Set[String]) extends SummingbirdConfig {

  def get(key: String) = {
    updates.get(key) match {
      case s @ Some(_) => s
      case None =>
        if (removes.contains(key))
          None
        else
          backingConfig.get(key)
    }
  }

  def put(key: String, v: AnyRef): WrappingConfig = {
    assert(v != null)
    this.copy(updates = (updates + (key -> v)), removes = (removes - key))
  }

  def remove(key: String): WrappingConfig = {
    this.copy(updates = (updates - key), removes = (removes + key))
  }

  def keys: Iterable[String] = {
    ((backingConfig.keys ++ updates.keys) -- removes)
  }
}