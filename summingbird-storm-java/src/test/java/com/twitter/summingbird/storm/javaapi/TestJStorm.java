package com.twitter.summingbird.storm.javaapi;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static scala.collection.JavaConversions.asScalaIterable;

import java.util.ArrayList;
import java.util.List;
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
import com.twitter.summingbird.storm.spout.TraversableSpout;
import com.twitter.util.Future;
import com.twitter.util.Future$;

public class TestJStorm {

  @Test
  public void test() throws Exception {
    final List<String> mapped = new ArrayList<String>();
    final List<String> written = new ArrayList<String>();
    @SuppressWarnings("unchecked")
    List<Tuple2<Timestamp, String>> input = asList(
        new Tuple2<Timestamp, String>(new Timestamp(System.currentTimeMillis()), "foo"),
        new Tuple2<Timestamp, String>(new Timestamp(System.currentTimeMillis()), "bar"),
        new Tuple2<Timestamp, String>(new Timestamp(System.currentTimeMillis()), "baz")
        );
    JStorm storm = JStorm.local();
    JProducer<Storm, String> source = JStorm.source(
        new TraversableSpout<Tuple2<Timestamp, String>>(asScalaIterable(input), "field"),
        JProducers.<SpoutParallelism>none());
    Sink<Storm, StormSink<String>, String> sink = JStorm.sink(new Callable<Function<String,Future<BoxedUnit>>>() {
      @Override
      public Function<String, Future<BoxedUnit>> call() throws Exception {
        return new Function<String, Future<BoxedUnit>>() {
          @Override
          public Future<BoxedUnit> apply(String s) {
            written.add(s);
            return Future$.MODULE$.Done();
          }
        };
      }
    });
    storm.run(source.map(new Function<String, String>() {
      @Override
      public String apply(String s) {
        mapped.add(s);
        return s;
      }
    }).write(sink), "myjob");
    assertEquals("mapped: " + mapped, 3, mapped.size());
    //		assertEquals("written: " + written, 3, written.size());
  }

}
