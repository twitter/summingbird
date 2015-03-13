package com.twitter.summingbird.storm.javaapi;

import static com.twitter.summingbird.javaapi.impl.JProducerImpl.toScala;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction0;

import com.twitter.chill.IKryoRegistrar;
import com.twitter.summingbird.Options;
import com.twitter.summingbird.SummingbirdConfig;
import com.twitter.summingbird.batch.BatchID;
import com.twitter.summingbird.batch.Timestamp;
import com.twitter.summingbird.javaapi.Buffer;
import com.twitter.summingbird.javaapi.Function;
import com.twitter.summingbird.javaapi.JProducer;
import com.twitter.summingbird.javaapi.JProducers;
import com.twitter.summingbird.javaapi.JTailProducer;
import com.twitter.summingbird.javaapi.Service;
import com.twitter.summingbird.javaapi.Sink;
import com.twitter.summingbird.javaapi.Source;
import com.twitter.summingbird.javaapi.Store;
import com.twitter.summingbird.storm.LocalStorm;
import com.twitter.summingbird.storm.PlannedTopology;
import com.twitter.summingbird.storm.RemoteStorm;
import com.twitter.summingbird.storm.SinkFn;
import com.twitter.summingbird.storm.SpoutSource;
import com.twitter.summingbird.storm.Storm;
import com.twitter.summingbird.storm.StormSink;
import com.twitter.summingbird.storm.StormSource;
import com.twitter.summingbird.online.OnlineServiceFactory;
import com.twitter.summingbird.online.ReadableServiceFactory;
import com.twitter.summingbird.online.MergeableStoreFactory;
import com.twitter.summingbird.online.option.SourceParallelism;
import com.twitter.tormenta.spout.Spout;
import com.twitter.util.Future;

public class JStorm {

  public static <T> JProducer<Storm, T> source(Spout<Tuple2<Timestamp, T>> spout, Option<SourceParallelism> parallelism) {
    return source(new SpoutSource<T>(spout, parallelism));
  }

  public static <T> JProducer<Storm, T> source(StormSource<T> source) {
	  return JProducers.<Storm, T>source(new Source<Storm, StormSource<T>, T>(source));
  }

  public static <K,V> Store<Storm, MergeableStoreFactory<Tuple2<K, BatchID>, V>, K, V> store(MergeableStoreFactory<Tuple2<K, BatchID>, V> store) {
    return new Store<Storm, MergeableStoreFactory<Tuple2<K, BatchID>, V>, K, V>(store);
  }

  public static <T> Sink<Storm, StormSink<T>, T> sink(final Callable<Function<T, com.twitter.util.Future<scala.runtime.BoxedUnit>>> sink) {
    return sink(new SinkFn<T>(new AbstractFunction0<Function1<T, com.twitter.util.Future<scala.runtime.BoxedUnit>>>() {
      @Override
      public Function1<T, Future<scala.runtime.BoxedUnit>> apply() {
        try {
          return toScala(sink.call());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }));
  }

  public static <T> Sink<Storm, StormSink<T>, T> sink(StormSink<T> sink) {
    return new Sink<Storm, StormSink<T>, T>(sink);
  }

  public static <K,V> Service<Storm, OnlineServiceFactory<K, V>, K, V> service(OnlineServiceFactory<K, V> service) {
    return new Service<Storm, OnlineServiceFactory<K, V>, K, V>(service);
  }

  public static <STORMBUFFER extends OnlineServiceFactory<K, V> & StormSink<Tuple2<K, V>>, K, V> Buffer<Storm, STORMBUFFER, K, V> buffer(STORMBUFFER service) {
    return new Buffer<Storm, STORMBUFFER, K, V>(service);
  }

  @SuppressWarnings("unchecked") // apply returns a raw Map ...
  private static <K, V> scala.collection.immutable.Map<K, V> toScalaMap(Map<K, V> map) {
    return scala.collection.immutable.Map$.MODULE$.apply(JavaConversions.asScalaMap(map).toSeq());
  }

  private static <T> scala.collection.immutable.List<T> toScalaList(List<T> list) {
    return JavaConversions.asScalaBuffer(list).toList();
  }

  public static JStorm local() {
    return local(Collections.<String, Options>emptyMap());
  }

  public static JStorm local(Map<String, Options> options) {
    return local(options, new Function<SummingbirdConfig, SummingbirdConfig>() {
		@Override
		public SummingbirdConfig apply(SummingbirdConfig config) {
			return config;
		}
	});
  }

  public static JStorm local(
		  Map<String, Options> options,
		  Function<SummingbirdConfig, SummingbirdConfig> transformConfig) {
    return local(options, transformConfig, Collections.<IKryoRegistrar>emptyList());
  }

  public static JStorm local(
		  Map<String, Options> options,
		  Function<SummingbirdConfig, SummingbirdConfig> transformConfig,
		  List<IKryoRegistrar> passedRegistrars) {
    return new JStorm(new LocalStorm(
            toScalaMap(options),
            toScala(transformConfig),
            toScalaList(passedRegistrars)));
  }

  public static JStorm remote(
		  Map<String, Options> options,
		  Function<SummingbirdConfig, SummingbirdConfig> transformConfig,
		  List<IKryoRegistrar> passedRegistrars) {
    return new JStorm(new RemoteStorm(
            toScalaMap(options),
            toScala(transformConfig),
            toScalaList(passedRegistrars)));
  }

  private Storm platform;

  private JStorm(Storm platform) {
	this.platform = platform;
  }

  /**
   * plans the job
   * @param tail
   * @return the planed producer
   */
  public <T> PlannedTopology plan(JTailProducer<Storm, T> tail) {
    return platform.plan(tail.unwrap());
  }

  /**
   * plans and runs a producer
   * @param tail the producer to run
   * @param jobName the name of the job
   */
  public void run(JTailProducer<Storm, ?> tail, String jobName) {
    run(plan(tail), jobName);
  }

  /**
   * runs a plan
   * @param plannedTopology the result of plan
   * @param jobName the name of the job
   */
  public void run(PlannedTopology plannedTopology, String jobName) {
    platform.run(plannedTopology, jobName);
  }

}
