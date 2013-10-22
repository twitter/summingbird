# summingbird #

## 0.2.4
* Fix off-by-one bug in scalding batched store: https://github.com/twitter/summingbird/pull/311

## 0.2.3
* Add convenience methods to Producer: https://github.com/twitter/summingbird/pull/307
* Massively optimize Scalding merge/sumByKey: https://github.com/twitter/summingbird/pull/303
* Improve the WaitingState state machine: https://github.com/twitter/summingbird/pull/302
* Move code for common realtime planners (storm + akka): https://github.com/twitter/summingbird/pull/299
* Fix an issue with source minify in scalding: https://github.com/twitter/summingbird/pull/298
* Improve scalding tests: https://github.com/twitter/summingbird/pull/296
* Improve storm tests: https://github.com/twitter/summingbird/pull/292
* Use an AnyVal-like class rather than java.util.Date: https://github.com/twitter/summingbird/pull/295
* improve Batcher documentation: https://github.com/twitter/summingbird/pull/293

## 0.2.2
* Use pipes instead of parentheses when naming online physical nodes: https://github.com/twitter/summingbird/pull/273
* Add Alsos in Producer2, OptProd2: https://github.com/twitter/summingbird/pull/275
* Multiple disjoint summers: https://github.com/twitter/summingbird/pull/274
* Make Tail sealed: https://github.com/twitter/summingbird/pull/276
* Scalding Laws not to use scalacheck: https://github.com/twitter/summingbird/pull/278
* Pull Online planner common code into core: https://github.com/twitter/summingbird/pull/281
* Use externalizer instead of meatlocker: https://github.com/twitter/summingbird/pull/282
* Standardize SourceBuilder Naming: https://github.com/twitter/summingbird/pull/285
* Add TailProducer, for graph ending point: https://github.com/twitter/summingbird/pull/286
* MemoryLaws not use scalacheck: https://github.com/twitter/summingbird/pull/287
* Added flatMapKeys to all platforms: https://github.com/twitter/summingbird/pull/288
* Added flatMapKeys to Builder API: https://github.com/twitter/summingbird/pull/290
* Add better type safety to Storm Spouts: https://github.com/twitter/summingbird/pull/289
* Add a streaming left join: https://github.com/twitter/summingbird/pull/291

## 0.2.1 
* Add support for map only jobs in producer api https://github.com/twitter/summingbird/pull/269
* Fixes naming of nodes, options are picked up correctly https://github.com/twitter/summingbird/pull/267
* Fix missing elements in case statements for applying online graph to storm https://github.com/twitter/summingbird/pull/272

## 0.2.0
* Bump the version numbers of the dependencies https://github.com/twitter/summingbird/pull/260
* Upgrade to Tormenta 0.5.2, Scalding 0.9.0, Bijection 0.5.3 betas: https://github.com/twitter/summingbird/pull/191
* Feature/storm new planner: https://github.com/twitter/summingbird/pull/250
* Feature/move dag to core: https://github.com/twitter/summingbird/pull/255


## 0.1.5
* Control how futures are collected in Client Store: https://github.com/twitter/summingbird/pull/254
* Producer[Platform, T] is covariant on T: https://github.com/twitter/summingbird/pull/251
* Improve testing generators: https://github.com/twitter/summingbird/pull/249
* Remove manifests from the core API: https://github.com/twitter/summingbird/pull/247
* Dot graphs of our storm plan (what SB nodes go to physical nodes): https://github.com/twitter/summingbird/pull/236

## 0.1.4

* Fix Storm Tests: https://github.com/twitter/summingbird/pull/227
* Custom --name flag for job name in builder api: https://github.com/twitter/summingbird/pull/226
* Remove SINK_ID reference from SourceBuilder: https://github.com/twitter/summingbird/pull/225
* Add Dot Graph Generation from SB Producer: https://github.com/twitter/summingbird/pull/223
* Push filters down to storm spout: https://github.com/twitter/summingbird/pull/224
* Revert StoreIntermediate to old approach: https://github.com/twitter/summingbird/pull/219
* Add in old SummingJoin: https://github.com/twitter/summingbird/pull/218

## 0.1.3

* Fix bug in storm planner's forking:  https://github.com/twitter/summingbird/pull/217
* Don't send empty multiGets to online service: https://github.com/twitter/summingbird/pull/209

## 0.1.2

* UTC Calendar Batcher: https://github.com/twitter/summingbird/pull/195
* Register injections in StormEnv: https://github.com/twitter/summingbird/pull/198
* Fix Forking bug in Storm planner: https://github.com/twitter/summingbird/pull/197
* Tidying of StormPlatform: https://github.com/twitter/summingbird/pull/199
* Add AnchorTuples option to Storm: https://github.com/twitter/summingbird/pull/200
* Allow NamedProducer after Summer in storm: https://github.com/twitter/summingbird/pull/202
* Optimization when using BijectedMonoid in a Scalding job: https://github.com/twitter/summingbird/pull/203
* Remove need for --initial-run (--start-time suffices): https://github.com/twitter/summingbird/pull/204

## 0.1.1

* Add --scalding.nothrowplan to not rethrow/exit with error on a flow plan error

## 0.1.0

* new, producer-based API
* Split Storm and Scalding platforms
* Added in-memory platform
* Complete DSL rebuild in anticipation of release.

## 0.0.4

* fix compiler-induced bug in BatchAggregatorJob.

## 0.0.3

* Ability to tee out data.

## 0.0.2

* PresentingStore
* Ability to join a Summingbird job against another SB job.

## 0.0.1

* Initial code push.
