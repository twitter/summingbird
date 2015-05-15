First pass at a spark platform for summingbird.
This is a work in progress, and is not really expected to work yet!

Outstanding issues:
 - Time based batching / BatchedStore equivalent not yet supported. Time is plubmed through everywhere and used for non commutative semigroups, but 
   batching doesn't really come into play yet.
 - Logic for finding the maximal covered timespan by various sources / stores is not yet implemented
 - Logic for lookups / leftjoins repsecting time (eg returning what a value was for a given key at a given time) is not yet implemented
 - Decide whether PlatformPlanner is useful or not
 - Test running real jobs on a real cluster (only tested once with wordcount)
 - SparkPlatform is stateful but shouldn't be
 - SparkPlatform submits blocking spark 'actions' in serial, should make (careful) use of a FuturePool instead, though we may run into spark 
   thread-safety issues there. I actually had this implemented but didn't want to spend the time finding out whether spark is threadsafe in 
   this way until I had something that worked in local mode first.
 - Writing the tests (SparkLaws) lead me to think that there should really be a platform agnostic set of laws, or at least a PlatformLaws base class. 
   There's a lot of overlap with the scalding platform in these tests. Having a PlatformLaws test suite would also better describe the contract and expected 
   behavior of all platforms.
 - There are currently no optimizations in the planning stage (except for commutativity not imposing a sort and using reduceByKey)
 - SparkPlatform is currently stateful but should be refactored to not be
