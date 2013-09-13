## Summingbird [![Build Status](https://secure.travis-ci.org/twitter/summingbird.png)](http://travis-ci.org/twitter/summingbird)

Summingbird is a library that lets you write MapReduce programs that look like native Scala or Java collection transformations and execute them on a number of well-known distributed MapReduce platforms, including [Storm](https://github.com/nathanmarz/storm) and [Scalding](https://github.com/twitter/scalding).

While a word-counting aggregation in pure Scala might look like this:

```scala
  def wordCount(source: Iterable[String], store: MutableMap[String, Long]) =
    source.flatMap { sentence =>
      toWords(sentence).map(_ -> 1L)
    }.foreach { case (k, v) => store.update(k, store.get(k) + v) }
```

Counting words in Summingbird looks like this:

```scala
  def wordCount[P <: Platform[P]]
    (source: Producer[P, String], store: P#Store[String, Long]) =
      source.flatMap { sentence =>
        toWords(sentence).map(_ -> 1L)
      }.sumByKey(store)
```

The logic is exactly the same, and the code is almost the same. The main difference is that you can execute the Summingbird program in "batch mode" (using [Scalding](https://github.com/twitter/scalding)), in "realtime mode" (using [Storm](https://github.com/nathanmarz/storm)), or on both Scalding and Storm in a hybrid batch/realtime mode that offers your application very attractive fault-tolerance properties.

Summingbird provides you with the primitives you need to build rock solid production systems.

## Community and Documentation

To learn more and find links to tutorials and information around the web, check out the [Summingbird Wiki](https://github.com/twitter/summingbird/wiki).

The latest ScalaDocs are hosted on Summingbird's [Github Project Page](http://twitter.github.io/summingbird).

Discussion occurs primarily on the [Summingbird mailing list](https://groups.google.com/forum/#!forum/summingbird). Issues should be reported on the GitHub issue tracker. Simpler issues appropriate for first-time contributors looking to help out are tagged "newbie".

Follow [@summingbird](https://twitter.com/summingbird) on Twitter for updates.

## Maven

Summingbird modules are published on maven central. The current groupid and version for all modules is, respectively, `"com.twitter"` and  `0.1.2`.

Current published artifacts are

* `summingbird-core_2.9.3`
* `summingbird-core_2.10`
* `summingbird-batch_2.9.3`
* `summingbird-batch_2.10`
* `summingbird-client_2.9.3`
* `summingbird-client_2.10`
* `summingbird-kryo_2.9.3`
* `summingbird-kryo_2.10`
* `summingbird-storm_2.9.3`
* `summingbird-storm_2.10`
* `summingbird-scalding_2.9.3`
* `summingbird-scalding_2.10`
* `summingbird-builder_2.9.3`
* `summingbird-builder_2.10`

The suffix denotes the scala version.

## Authors

* Oscar Boykin <https://twitter.com/posco>
* Sam Ritchie <https://twitter.com/sritchie>
* Ashutosh Singhal <https://twitter.com/daashu>

## License

Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
