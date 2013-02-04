package com.twitter.summingbird.bijection

import com.twitter.util.Encoder

/**
 *  @author Oscar Boykin
 *  @author Sam Ritchie
 *
 * The HashEncoder provides an extensible way to hash a byte array. Summingbird makes use
 * of the HashEncoder in the MemcacheStore; keys in Memcache are restricted to 256
 * characters, and the HashEncoder allows a way around this limit with very little
 * chance of key collision.
 */

// See this reference for other algorithm names:
// http://docs.oracle.com/javase/1.4.2/docs/guide/security/CryptoSpec.html#AppA

// TODO: Convert this to use the new Hasher trait.

class HashEncoder(hashFunc: String = "SHA-256") extends Encoder[Array[Byte],Array[Byte]] {
  def encode(bytes: Array[Byte]): Array[Byte] = {
    val md = java.security.MessageDigest.getInstance(hashFunc)
    md.digest(bytes)
  }
}

object HashEncoder {
  def apply() = new HashEncoder
  def apply(hashFunc: String) = new HashEncoder(hashFunc)
}
