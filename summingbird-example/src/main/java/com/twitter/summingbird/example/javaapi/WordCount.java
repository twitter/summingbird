package com.twitter.summingbird.example.javaapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.twitter.summingbird.*;
import com.twitter.summingbird.javaapi.*;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.TraversableOnce;
import scala.runtime.AbstractFunction1;
import twitter4j.Status;

import com.twitter.algebird.Semigroup;
import com.twitter.algebird.Semigroup$;

public class WordCount {

  public static List<String> tokenize(String text) {
    return Arrays.asList(
        text.toLowerCase()
            .replaceAll("[^a-zA-Z0-9\\s]", "")
            .split("\\s+"));
   }

  @SuppressWarnings("unchecked") // have to use MODULE$ and it's a Semigroup<Object>
  private static Semigroup<Long> sg = (Semigroup<Long>)(Semigroup<?>)Semigroup$.MODULE$.longSemigroup();

  public static <P extends Platform<P>> void wordCount(Producer<P, Status> source, Object store /* store is typed Object */) {
    Producer$.MODULE$.<P, String, Long>toKeyed( // we have to call toKeyed around the Producer
        source.filter(new AbstractFunction1<Status, Object>() { // filter takes a function that returns Object
          public Object apply(Status s) {
            return s.getText() != null;
          }
        }).flatMap(new AbstractFunction1<Status, TraversableOnce<Tuple2<String,Long>>>() {
          public TraversableOnce<Tuple2<String,Long>> apply(Status s) {
            List<Tuple2<String,Long>> tokens = new ArrayList<Tuple2<String,Long>>();
            for (String token : tokenize(s.getText())) {
              tokens.add(new Tuple2<String,Long>(token, 1L));
            }
            return JavaConversions.collectionAsScalaIterable(tokens); // have to convert collection
          }
   })).sumByKey(store, sg); // store is typed Object
  }

  public static <P extends Platform<P>> void wordCount2(JProducer<P, Status> source, Store<P, ?, String, Long> store) {
    source.filter(new Predicate<Status>() {
      public boolean test(Status s) {
        return s.getText() != null;
      }
    }).flatMap(new Function<Status, Iterable<String>>() {
    public Iterable<String> apply(Status s) {
        return tokenize(s.getText());
    }
    }).mapToKeyed(new Function<String, Tuple2<String,Long>>() {
      public Tuple2<String,Long> apply(String token) {
        return new Tuple2<String, Long>(token, 1L);
      }
    }).sumByKey(store, sg);
  }
}
