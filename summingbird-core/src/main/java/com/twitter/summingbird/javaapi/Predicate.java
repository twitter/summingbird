package com.twitter.summingbird.javaapi;

public interface Predicate<T> {
  boolean test(T v);
}
