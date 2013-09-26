# summingbird #

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
