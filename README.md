## Summingbird [![Build Status](https://secure.travis-ci.org/twitter/summingbird.png)](http://travis-ci.org/twitter/summingbird)

Summingbird is a library that lets you write MapReduce programs that look like native Scala or Java collection transformations and execute them on a number of well-known distributed MapReduce platforms, including [Storm](https://github.com/nathanmarz/storm) and [Scalding](https://github.com/twitter/scalding).

![Summingbird Logo](https://raw.github.com/twitter/summingbird/develop/logo/summingbird_logo.png)

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

## Getting Started: Word Count with Twitter

The `summingbird-example` project allows you to run the wordcount program above on a sample of Twitter data using a local Storm topology and memcache instance. You can find the actual job definition in [ExampleJob.scala](https://github.com/twitter/summingbird/blob/develop/summingbird-example/src/main/scala/com/twitter/summingbird/example/ExampleJob.scala).

First, make sure you have `memcached` installed locally. If not, if you're on OS X, you can get it by installing [Homebrew](http://brew.sh/) and running this command in a shell:

```bash
brew install memcached
```

When this is finished, run the `memcached` command in a separate terminal.

Now you'll need to set up access to the Twitter Streaming API. [This blog post](http://tugdualgrall.blogspot.com/2012/11/couchbase-create-large-dataset-using.html) has a great walkthrough, so open that page, head over to https://dev.twitter.com/ and get your various keys and tokens. Once you have these, clone the Summingbird repository:

```bash
git clone https://github.com/twitter/summingbird.git
cd summingbird
```

And open [StormRunner.scala](https://github.com/twitter/summingbird/blob/develop/summingbird-example/src/main/scala/com/twitter/summingbird/example/StormRunner.scala) in your editor. Replace the dummy variables under `config` variable with your auth tokens:

```scala
lazy val config = new ConfigurationBuilder()
    .setOAuthConsumerKey("mykey")
    .setOAuthConsumerSecret("mysecret")
    .setOAuthAccessToken("token")
    .setOAuthAccessTokenSecret("tokensecret")
    .setJSONStoreEnabled(true) // required for JSON serialization
    .build
```

You're all ready to go! Now it's time to unleash Storm on your Twitter stream. Make sure the `memcached` terminal is still open, then start Storm from the `summingbird` directory:

```bash
./sbt "summingbird-example/run --local"
```

Storm should puke out a bunch of output, then stabilize and hang. This means that Storm is updating your local memcache instance with counts of every word that it sees in each tweet.

To query the aggregate results in Memcached, you'll need to open an SBT repl in a new terminal:

```bash
./sbt summingbird-example/console
```

At the launched repl, run the following:

```scala
scala> import com.twitter.summingbird.example._
import com.twitter.summingbird.example._

scala> StormRunner.lookup("i")
<memcache store loading elided>
res0: Option[Long] = Some(5)

scala> StormRunner.lookup("i")
res1: Option[Long] = Some(52)
```

Boom. Counts for the word `"i"` are growing in realtime.

See the [wiki page](https://github.com/twitter/summingbird/wiki/Getting-started-with-summingbird-example) for a more detailed explanation of the configuration required to get this job up and running and some ideas for where to go next.

## Community and Documentation

This, and all [github.com/twitter](https://github.com/twitter) projects, are under the [Twitter Open Source Code of Conduct](https://engineering.twitter.com/opensource/code-of-conduct). Additionally, see the [Typelevel Code of Conduct](http://typelevel.org/conduct) for specific examples of harassing behavior that are not tolerated.

To learn more and find links to tutorials and information around the web, check out the [Summingbird Wiki](https://github.com/twitter/summingbird/wiki).

The latest ScalaDocs are hosted on Summingbird's [Github Project Page](http://twitter.github.io/summingbird).

Discussion occurs primarily on the [Summingbird mailing list](https://groups.google.com/forum/#!forum/summingbird). Issues should be reported on the GitHub issue tracker. Simpler issues appropriate for first-time contributors looking to help out are tagged "newbie".

IRC: freenode channel #summingbird

Follow [@summingbird](https://twitter.com/summingbird) on Twitter for updates.

Please feel free to use the beautiful [Summingbird logo](https://drive.google.com/folderview?id=0B3i3pDi3yVgNMHV0TXVkTGZteWM&usp=sharing) artwork anywhere.

## Maven

Summingbird modules are published on maven central. The current groupid and version for all modules is, respectively, `"com.twitter"` and  `0.9.1`.

Current published artifacts are

* `summingbird-core_2.11`
* `summingbird-core_2.10`
* `summingbird-batch_2.11`
* `summingbird-batch_2.10`
* `summingbird-client_2.11`
* `summingbird-client_2.10`
* `summingbird-storm_2.11`
* `summingbird-storm_2.10`
* `summingbird-scalding_2.11`
* `summingbird-scalding_2.10`
* `summingbird-builder_2.11`
* `summingbird-builder_2.10`

The suffix denotes the scala version.

## Authors (alphabetically)

* Oscar Boykin <https://twitter.com/posco>
* Ian O'Connell <https://twitter.com/0x138>
* Sam Ritchie <https://twitter.com/sritchie>
* Ashutosh Singhal <https://twitter.com/daashu>

## License

Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
