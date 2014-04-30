package com.twitter.summingbird.example.java;

import java.util.ArrayList;
import java.util.List;

import com.twitter.summingbird.*;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.TraversableOnce;
import scala.runtime.AbstractFunction1;
import twitter4j.Status;
import com.twitter.algebird.Semigroup$;

public class WordCount {

  public static String[] tokenize(String text) {
    return text.toLowerCase()
      .replaceAll("[^a-zA-Z0-9\\s]", "")
      .split("\\s+");
   }

  public static <P extends Platform<P>> void wordCount(Producer<P, Status> source, Object store) {
    source.filter(new AbstractFunction1<Status, Object>() {
      public Object apply(Status s) {
        return s.getText() != null;
      }
    }).flatMap(new AbstractFunction1<Status, TraversableOnce<Tuple2<String,Long>>>() {
      public TraversableOnce<Tuple2<String,Long>> apply(Status s) {
        List<Tuple2<String,Long>> tokens = new ArrayList<Tuple2<String,Long>>();
        for (String token : tokenize(s.getText())) {
          tokens.add(new Tuple2<String,Long>(token, 1L));
        }
        return JavaConversions.collectionAsScalaIterable(tokens);
     }
   }).asKeyed().sumByKey(store, Semigroup$.MODULE$.longSemigroup());
  }
}
