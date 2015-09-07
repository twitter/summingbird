package com.twitter.summingbird.example

import org.scalatest.WordSpec

class Smoketest extends WordSpec {

  "The memcache storage" should {
    "be able to be created" in {
      val stringLongStore =
        Memcache.mergeable[String, Long]("urlCount")
      assert(1 == 1)
    }
  }
}
