package com.twitter.summingbird

import com.twitter.storehaus.ReadableStore

/**
  * Package containing the Summingbird Storm platform.
  */
package object storm {
  type StoreFactory[K, V] = () => ReadableStore[K, V]
}
