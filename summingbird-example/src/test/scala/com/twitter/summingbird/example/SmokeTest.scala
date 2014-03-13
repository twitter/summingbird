package com.twitter.summingbird.example

import org.specs2.mutable._

class Smoketest extends Specification {

  "The memcache storage" should {
    "be able to be created" in {
      val stringLongStore =
        Memcache.mergeable[String, Long]("urlCount")
      1 must beEqualTo(1)
    }
  }
}
