package com.twitter.summingbird.memory.javaapi;

import static com.twitter.summingbird.javaapi.JProducers.some;
import static com.twitter.summingbird.javaapi.JProducers.toKeyed;
import static com.twitter.summingbird.memory.javaapi.JMemory.service;
import static com.twitter.summingbird.memory.javaapi.JMemory.source;
import static com.twitter.summingbird.memory.javaapi.JMemory.store;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import scala.Function1;
import scala.Option;
import scala.Some;
import scala.Tuple2;

import com.twitter.algebird.Semigroup;
import com.twitter.summingbird.javaapi.Function;
import com.twitter.summingbird.javaapi.JProducer;
import com.twitter.summingbird.javaapi.JProducers;
import com.twitter.summingbird.javaapi.JSummer;
import com.twitter.summingbird.javaapi.Predicate;
import com.twitter.summingbird.javaapi.Service;
import com.twitter.summingbird.javaapi.Sink;
import com.twitter.summingbird.memory.Memory;

public class TestJMemory {

  private static final Semigroup<Long> sg = JProducers.semigroup(Long.class);
  private static final String[] INPUT = { "one", "two", "three" };
  private static final Integer[] LENGTH = { 3, 3, 5 };
  private static final String[] LESS_THAN_4 = { "one", "two" };
  private static final String[] FLATTENED = { "o", "n", "e", "t", "w", "o", "t", "h", "r", "e", "e" };

  private static final JProducer<Memory, String> SOURCE = source(asList(INPUT));

  private static final Service<Memory, Function1<String, Option<Integer>>, String, Integer> LENGTH_SERVICE = service(new Function<String, Option<Integer>>() {
    public Option<Integer> apply(String p) {
      return new Some<Integer>(p.length());
    }
  });

  private static final Predicate<String> STRING_LESS_THAN_4 = new Predicate<String>() {
    @Override
    public boolean test(String v) {
      return v.length() < 4;
    }
  };

  private static final Function<String, Integer> TO_LENGTH = new Function<String, Integer>() {
    public Integer apply(String p) {
      return p.length();
    }
  };

  private static final Function<String, Tuple2<String, Long>> TO_KEY_1 = new Function<String, Tuple2<String, Long>>() {
    public Tuple2<String, Long> apply(String p) {
      return new Tuple2<String, Long>(p, 1L);
    }
  };

  private static final Function<String, List<String>> SPLIT = new Function<String, List<String>>() {
    public List<String> apply(String p) {
      return Arrays.asList(p.split(""));
    }
  };

  private static final Function<Option<Integer>, Integer> FROM_OPTION = new Function<Option<Integer>, Integer>() {
    public Integer apply(Option<Integer> p) {
      return p.get();
    }
  };

  private JMemory platform = new JMemory();

  private <T> Sink<Memory, ?, T> sink(final List<T> out) {
    Sink<Memory, Function1<T, Void>, T> sink = JMemory.sink(new JSink<T>() {
      @Override
      public void write(T p) {
        out.add(p);
      }
    });
    return sink;
  }

  private <T> List<T> plan(JProducer<Memory, T> job, List<T> out) {
    return platform.plan(job.write(sink(out)));
  }

  private <T> void validateSize(JProducer<Memory, T> job, int size) {
    List<T> out = new ArrayList<T>();
    List<T> plan = plan(job, out);
    assertEquals("plan: " + plan, size, plan.size());
    assertEquals("out: " + out, size, out.size());
  }

  private <T> void validateResult(JProducer<Memory, T> job, T[] result) {
    List<T> out = new ArrayList<T>();
    List<T> plan = plan(job, out);
    assertEquals("plan: " + plan, asList(result), plan);
    assertEquals("out: " + out, asList(result), out);
  }

  @Test
  public void testMap() {
    validateResult(
        SOURCE.map(TO_LENGTH),
        LENGTH);
  }

  @Test
  public void testFilter() {
    validateResult(
        SOURCE.filter(STRING_LESS_THAN_4),
        LESS_THAN_4);
  }

  @Test
  public void testFlatMap() {
    validateResult(SOURCE.flatMap(SPLIT), FLATTENED);
  }

  @Test
  public void testOptionMap() {
    validateResult(SOURCE.optionMap(new Function<String, Option<String>>() {
      @Override
      public Option<String> apply(String v) {
        return v.length() < 4 ? some(v) : JProducers.<String>none();
      }
    }), LESS_THAN_4);
  }

  @Test
  public void testSumByKey() {
    HashMap<String, Long> store = new HashMap<String, Long>();
    validateSize(SOURCE.merge(SOURCE).mapToKeyed(TO_KEY_1).sumByKey(store(store), sg), 6);
    assertEquals("store: " + store, 3, store.size());
    for (Long count : store.values()) {
      assertEquals(2L, count.longValue());
    }
  }

  @Test
  public void testLookup() {
    validateResult(
        SOURCE.lookup(LENGTH_SERVICE).values().map(FROM_OPTION),
        LENGTH);
  }

  @Test
  public void testFilterKeys() {
    validateResult(
        SOURCE.mapToKeyed(TO_KEY_1).filterKeys(STRING_LESS_THAN_4).keys(),
        LESS_THAN_4);
  }

  @Test
  public void testFilterValues() {
    validateResult(
        SOURCE.mapToKeyed(TO_KEY_1).swap().filterValues(STRING_LESS_THAN_4).values(),
        LESS_THAN_4);
  }

  @Test
  public void testFlatMapKeys() {
    validateResult(
        SOURCE.mapToKeyed(TO_KEY_1).flatMapKeys(SPLIT).keys(),
        FLATTENED);
  }

  @Test
  public void testFlatMapValues() {
    validateResult(
        SOURCE.mapToKeyed(TO_KEY_1).swap().flatMapValues(SPLIT).values(),
        FLATTENED);
  }

  @Test
  public void testMapKeys() {
    validateResult(
        SOURCE.mapToKeyed(TO_KEY_1).mapKeys(TO_LENGTH).keys(),
        LENGTH);
  }

  @Test
  public void testMapValues() {
    validateResult(
        SOURCE.mapToKeyed(TO_KEY_1).swap().mapValues(TO_LENGTH).values(),
        LENGTH);
  }

  @Test
  public void testLeftJoin() {
    validateResult(
        toKeyed(SOURCE.mapToKeyed(TO_KEY_1).leftJoin(LENGTH_SERVICE).values())
        .values().map(FROM_OPTION),
        LENGTH);
  }

  @Test
  public void testAlso() {
    HashMap<String, Long> store = new HashMap<String, Long>();
    validateSize(SOURCE.mapToKeyed(TO_KEY_1).sumByKey(store(store), sg).also(SOURCE), 3);
    assertEquals("store: " + store, 3, store.size());
    for (Long count : store.values()) {
      assertEquals(1L, count.longValue());
    }
  }

  @Test
  public void testAlsoTail() {
    HashMap<String, Long> store = new HashMap<String, Long>();
    JSummer<Memory, String, Long> sumByKey1 = SOURCE.mapToKeyed(TO_KEY_1).sumByKey(store(store), sg);
    JSummer<Memory, String, Long> sumByKey2 = SOURCE.mapToKeyed(TO_KEY_1).sumByKey(store(store), sg);
    validateSize(sumByKey1.also(sumByKey2).also(SOURCE), 3);
    assertEquals("store: " + store, 3, store.size());
    for (Long count : store.values()) {
      assertEquals(2L, count.longValue());
    }
  }

}
