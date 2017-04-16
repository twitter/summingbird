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

package com.twitter.summingbird.store

/**
  * This trait is useful for extending a ClientStore that is thin but needs to have its own type.
  */
trait ProxyClientStore[K, V] extends ClientStore[K, V] {
  def proxy: ClientStore[K, V]
  override def multiGet[K1 <: K](ks: Set[K1]) = proxy.multiGet(ks)
  override def toString = "ProxyClientStore(proxyingFor=%s)".format(proxy.toString)
 }
