package com.twitter.summingbird.storm.javaapi;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.junit.Test;

import scala.Tuple2;
import scala.runtime.BoxedUnit;

import com.twitter.summingbird.batch.Timestamp;
import com.twitter.summingbird.javaapi.Function;
import com.twitter.summingbird.javaapi.JProducer;
import com.twitter.summingbird.javaapi.JProducers;
import com.twitter.summingbird.javaapi.Sink;
import com.twitter.summingbird.storm.Storm;
import com.twitter.summingbird.storm.StormSink;
import com.twitter.summingbird.storm.option.SpoutParallelism;
import com.twitter.summingbird.storm.spout.FixedTupleSpout;
import com.twitter.tormenta.spout.Spout;
import com.twitter.util.Future;


public class TestJStorm {

	String value;

	@Test
	public void test() {
		JStorm storm = JStorm.local();
		JProducer<Storm, String> source = JStorm.source(
				(Spout<Tuple2<Timestamp, String>>) new FixedTupleSpout(Arrays.<Tuple2<Timestamp, String>>asList(new Tuple2<Timestamp, String>(null, "foo"))),
				JProducers.<SpoutParallelism>none());
		Sink<Storm, StormSink<String>, String> sink = JStorm.sink(new Callable<Function<String,Future<BoxedUnit>>>() {
			@Override
			public Function<String, Future<BoxedUnit>> call() throws Exception {
				return new Function<String, Future<BoxedUnit>>() {
					@Override
					public Future<BoxedUnit> apply(String p) {
						value = p;
						return null;
					}
				};
			}
		});
		storm.run(source.write(sink), "foo");
		assertEquals("foo", value);
	}
}
