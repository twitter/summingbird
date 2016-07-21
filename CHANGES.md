# Summingbird #

## 0.9.1
* Fixes a bug in the LTuple2 class. Add a test for it: https://github.com/twitter/summingbird/pull/632

## 0.9.0
* Removing internal config setup from scalding platform: https://github.com/twitter/summingbird/pull/629
* Remove store that no one seems to use and has no tests: https://github.com/twitter/summingbird/pull/630
* Ianoc/optimization changes: https://github.com/twitter/summingbird/pull/628
* Fixes tests: https://github.com/twitter/summingbird/pull/626
* resilient to all stores being complete: https://github.com/twitter/summingbird/pull/627
* Resilient to a store being ahead: https://github.com/twitter/summingbird/pull/625
* move to scala test: https://github.com/twitter/summingbird/pull/621
* Killing off Summingbird Java: https://github.com/twitter/summingbird/pull/624
* Kill summingbird spark: https://github.com/twitter/summingbird/pull/622
* Bumping sbt versions: https://github.com/twitter/summingbird/pull/623
* Fixed erroneous comment in ClientStore&#39;s merge description: https://github.com/twitter/summingbird/pull/620

## 0.8.0
* bumping Scalding to 0.15.0, algebird to 0.10.1

## 0.7.0
* Registering summingbird counters with tormenta spouts #553
* Add counters to default summers #571
* Fixes the types off the summingbird java storm api #601
* Adding explicit hadoop deps; dfs-datastores to 1.3.6 #592
* Replace deprecated ClassManifest and erasure #608
* Spark monad #607
* replace deprecated erasure by runtimeClass #609
* Revert "Remove StripNameNodes" #610
* refactored core-tests into separate sub-project #593
* Fix checking delta batch in readDeltaTimestamps in batched store when merging #612
* ensure at least one batch before merging #613
* MergeableStoreFactory extends java.io.Serializable #616
* Spout storm metrics foreach #617
* upgrade scalacheck #615
* Use latest bijection, algebird, scalding, storehaus, chill, and tormenta

## 0.6.0
* Use latest bijection, algebird, scalding, storehaus, chill, and tormenta https://github.com/twitter/summingbird/pull/565
* Remove scala 2.9 support https://github.com/twitter/summingbird/pull/565
* Add counters to default summers https://github.com/twitter/summingbird/pull/571
* Registering summingbird counters with tormenta spouts https://github.com/twitter/summingbird/pull/553
* Remove StripNameNodes: https://github.com/twitter/summingbird/pull/587
* Revert unneeded wrapper in DagOptimizer: https://github.com/twitter/summingbird/pull/584
* Make some classes public so we can make custom platforms: https://github.com/twitter/summingbird/pull/564
* IdentityKeyedProducer in ValueFlatMapToFlatMap Dag Optimizer rule: https://github.com/twitter/summingbird/pull/580
* Lazy store init for CombinedServiceStoreFactory: https://github.com/twitter/summingbird/pull/582
* Pass the reducers option down to leftJoin: https://github.com/twitter/summingbird/pull/578
* Rename readBatched to readAfterLastBatch: https://github.com/twitter/summingbird/pull/577
* Fixes BatchedStore interval calculation: https://github.com/twitter/summingbird/pull/573
* Support for creating a store-service for Storm platform: https://github.com/twitter/summingbird/pull/563
* Support leftJoin against a store in offline platform: https://github.com/twitter/summingbird/pull/557
* Counters for ConcurrentMemory platform: https://github.com/twitter/summingbird/pull/550
* Summingbird Storm/Online refactor: https://github.com/twitter/summingbird/pull/544
* Memory platform counters: https://github.com/twitter/summingbird/pull/548
* Example of using DagOptimizer: https://github.com/twitter/summingbird/pull/538

## 0.5.1
* Change the javac options for doc generation in java to be successful. (Blocks maven publishing)

## 0.5.0
* Bumps version of other projects. Changes to compile with latest scalding...: https://github.com/twitter/summingbird/pull/533
* Safely checking for the internal storm Implementation detail: https://github.com/twitter/summingbird/pull/528
* add implicit Successible and Predecessible for Timestamp: https://github.com/twitter/summingbird/pull/530
* Removes redundant Future.unit call: https://github.com/twitter/summingbird/pull/526
* Optimize CalendarBatcher a bit: https://github.com/twitter/summingbird/pull/527
* Fixes java publishing to not error in the doc generation/publishing -- i...: https://github.com/twitter/summingbird/pull/519
* Fix online plan bug with multiple serial summers and certain no-ops: https://github.com/twitter/summingbird/pull/524
* Fixing tormentaSpout.registerMetric call in scheduleSpout: https://github.com/twitter/summingbird/pull/521
* Use scalariform: https://github.com/twitter/summingbird/pull/520
* Adds a test for a bug if a flatMap immediately before a leftJoin produce...: https://github.com/twitter/summingbird/pull/518
* Fixed Storm stats bug with Promise: https://github.com/twitter/summingbird/pull/514
* Platform-independent stats storm scalding: https://github.com/twitter/summingbird/pull/503
* Experimental Java API and example: https://github.com/twitter/summingbird/pull/499
* First pass at a spark platform for summingbird: https://github.com/twitter/summingbird/pull/502
* Add summingbird logo: https://github.com/twitter/summingbird/pull/504
* expose flow object to mutate function: https://github.com/twitter/summingbird/pull/501
* Support java6 with our one java file: https://github.com/twitter/summingbird/pull/498
* New async summers from algebird: https://github.com/twitter/summingbird/pull/497
* Feature/stat store: https://github.com/twitter/summingbird/pull/495
* Add some option documentation: https://github.com/twitter/summingbird/pull/491
* Yet another cache: https://github.com/twitter/summingbird/pull/470
* Monoid to semigroup: https://github.com/twitter/summingbird/pull/487

## 0.4.2
* Use scalding version 0.9.0rc17 which has some bug fixes
* Add fully satisfiable flag to UniqueKeyedService: https://github.com/twitter/summingbird/pull/489
* Mark the scalding platform as serializable, but also don't grab the platform in the scalding env: https://github.com/twitter/summingbird/pull/485
* smoketest displaying error running example: https://github.com/twitter/summingbird/pull/483

## 0.4.1
* Release tool failure fix from 0.4.0 release.

## 0.4.0
* Add/use the multiplier option: https://github.com/twitter/summingbird/pull/481
* add variations on pipeFactory to allow a map function before time extrac...: https://github.com/twitter/summingbird/pull/482
* Make HDFSState public: https://github.com/twitter/summingbird/pull/478
* Feature/ianoc frozen keys: https://github.com/twitter/summingbird/pull/477
* Piping prune function through the VersionedStore factory method: https://github.com/twitter/summingbird/pull/476
* Remove deprecation indirections: https://github.com/twitter/summingbird/pull/472
* fix unidoc target: https://github.com/twitter/summingbird/pull/469
* Add hadoop2 compression options: https://github.com/twitter/summingbird/pull/466
* Few more profiler noticed changes, mostly Future.collect indexedSeq&#39;s. O...: https://github.com/twitter/summingbird/pull/465
* Add deprecation: https://github.com/twitter/summingbird/pull/463
* Vagrant folder added to .gitignore: https://github.com/twitter/summingbird/pull/464
* Manually block up sections of output from caches into lists to avoid flo...: https://github.com/twitter/summingbird/pull/453
* Fixes bug that meant storm tests weren't running: https://github.com/twitter/summingbird/pull/460
* Factor batch from online: https://github.com/twitter/summingbird/pull/455
* This moves from using the completeTopology approach to running it normal...: https://github.com/twitter/summingbird/pull/458
* Code is called heavily, and in tests where we do it 100's of times it wa...: https://github.com/twitter/summingbird/pull/457
* Clean up a little bit of this code. Uses seq/traversable once in some ap...: https://github.com/twitter/summingbird/pull/456
* This refactors time out of several places inside the online code. Allows...: https://github.com/twitter/summingbird/pull/454
* Move the hadoop defaults into scalding platform: https://github.com/twitter/summingbird/pull/452
* Feature/add option to tune output sizes online: https://github.com/twitter/summingbird/pull/451
* Move to using Mergable from storehaus instead of mergable store.: https://github.com/twitter/summingbird/pull/450
* Feature/state deprecated ptrs: https://github.com/twitter/summingbird/pull/449
* Adds BatchID interval to TS interval: https://github.com/twitter/summingbird/pull/443
* Adds a scalding-test project: https://github.com/twitter/summingbird/pull/447
* Make Commutativity settings a case object: https://github.com/twitter/summingbird/pull/448
* Splits the storm test code out into its own package to help end users be...: https://github.com/twitter/summingbird/pull/442
* Add toString to InitialBatchedStore: https://github.com/twitter/summingbird/pull/444
* Feature/long to timestamp offline: https://github.com/twitter/summingbird/pull/439
* Feature/add pruning support: https://github.com/twitter/summingbird/pull/435
* Split the scalding platform out into packages: https://github.com/twitter/summingbird/pull/434
* Add mima command support: https://github.com/twitter/summingbird/pull/427
* Add PipeFactoryOps, with mapElements, flatMapElements and mapPipe: https://github.com/twitter/summingbird/pull/432
* Feature/move code from scalding: https://github.com/twitter/summingbird/pull/424

## 0.3.3
* Depend on proper Tormenta version (fix semver hiccup)

## 0.3.2
* We don't want to hit runningState.fail twice: https://github.com/twitter/summingbird/pull/425
* also tail producer support: https://github.com/twitter/summingbird/pull/423
* fix merge planner bug: https://github.com/twitter/summingbird/pull/417
* shard fm: https://github.com/twitter/summingbird/pull/416
* Add sinking to stores: https://github.com/twitter/summingbird/pull/408
* Add NormalScaldingStore which contains only (K, V) pairs: https://github.com/twitter/summingbird/pull/407

## 0.3.1
* Bump the chill version - makes storm provided: https://github.com/twitter/summingbird/pull/406
* sort by batch if monoid is commutative: https://github.com/twitter/summingbird/pull/411
* Fix README to properly run Storm locally: https://github.com/twitter/summingbird/pull/412
* Feature/async summer store: https://github.com/twitter/summingbird/pull/400

## 0.3.0
* Add serializers for BatchID, Timestamp: https://github.com/twitter/summingbird/pull/350
* ADd a means to set defaults outside the planner: https://github.com/twitter/summingbird/pull/351
* fix bugs hdfs: https://github.com/twitter/summingbird/pull/353
* add default option setting: https://github.com/twitter/summingbird/pull/356
* Add options tests: https://github.com/twitter/summingbird/pull/335
* Add some hadoop defaults to the builder API: https://github.com/twitter/summingbird/pull/359
* Cleanup the Storm API a bit: https://github.com/twitter/summingbird/pull/360
* Put all Collector actions in one execute: https://github.com/twitter/summingbird/pull/365
* Producer API executors for storm and scalding: https://github.com/twitter/summingbird/pull/367
* standardize get or else scalding: https://github.com/twitter/summingbird/pull/363
* Fixes ticks: https://github.com/twitter/summingbird/pull/368
* spout metrics: https://github.com/twitter/summingbird/pull/344
* Add the summingbird prefix to the graphs: https://github.com/twitter/summingbird/pull/370
* add localOrShuffle: https://github.com/twitter/summingbird/pull/371
* Make the StormMetric Covariant: https://github.com/twitter/summingbird/pull/373
* Fix depreciation to be from sink: https://github.com/twitter/summingbird/pull/375
* add options to cfg: https://github.com/twitter/summingbird/pull/376
* Null out Tuple.values for GC: https://github.com/twitter/summingbird/pull/374
* Add timestamps to hadoop configs: https://github.com/twitter/summingbird/pull/377/files
* add Options.toString: https://github.com/twitter/summingbird/pull/378
* Fix out of sync versions to keep: https://github.com/twitter/summingbird/pull/379
* Feature fix ffm: https://github.com/twitter/summingbird/pull/381
* Add an await timeout: https://github.com/twitter/summingbird/pull/380
* Add local or shuffle option: https://github.com/twitter/summingbird/pull/384
* Add dequeueAll to Queue: https://github.com/twitter/summingbird/pull/386
* Add in log4j and slf4j to the example: https://github.com/twitter/summingbird/pull/392
* Fixes naming in bolts for options to work right after plan: https://github.com/twitter/summingbird/pull/391
* Add error checking to the cache size: https://github.com/twitter/summingbird/pull/393
* Add more storm tests, add some new test graphs: https://github.com/twitter/summingbird/pull/394
* Proxy the select correctly on initialbatch: https://github.com/twitter/summingbird/pull/389
* Refactors the bolts from storm to be independent workers in online: https://github.com/twitter/summingbird/pull/395
* Pluggable caches in FinalFlatMap: https://github.com/twitter/summingbird/pull/396
* Uses multimerge: https://github.com/twitter/summingbird/pull/399
* ack on entry option: https://github.com/twitter/summingbird/pull/398

## 0.2.5
* Fix issue with externalizer: https://github.com/twitter/summingbird/pull/316
* Initial implementation of checkpoint state: https://github.com/twitter/summingbird/pull/315
* Move to sbt 0.13: https://github.com/twitter/summingbird/pull/322
* Move Dependants common code to graph: https://github.com/twitter/summingbird/pull/321
* CheckpointState -> HDFSState: https://github.com/twitter/summingbird/pull/318
* Change depthFirstOf to use List instead of Vector: https://github.com/twitter/summingbird/pull/323
* Add an optional parameter to set default parallelism for a storm source: https://github.com/twitter/summingbird/pull/319
* Add a method to prepare for optimizing FlatMapKeys: https://github.com/twitter/summingbird/pull/305
* fix the increased output issue: https://github.com/twitter/summingbird/pull/327
* Optimize iterator sums: https://github.com/twitter/summingbird/pull/326
* Make sure to register BatchID and Timespan: https://github.com/twitter/summingbird/pull/324
* Sum by key test: https://github.com/twitter/summingbird/pull/330
* Add slf4j logging to scalding: https://github.com/twitter/summingbird/pull/331
* Move the versioned store to scalding package: https://github.com/twitter/summingbird/pull/332
* storm logging: https://github.com/twitter/summingbird/pull/333
* Adds/extends some storm option tests: https://github.com/twitter/summingbird/pull/334
* Make VersionedState fail-safe: https://github.com/twitter/summingbird/pull/337
* Throw on Storm Submission w/ no online store: https://github.com/twitter/summingbird/pull/338
* General graph support online: https://github.com/twitter/summingbird/pull/340
* Summingbird config: https://github.com/twitter/summingbird/pull/339

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
