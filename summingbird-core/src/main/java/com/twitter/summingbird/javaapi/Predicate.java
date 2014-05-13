package com.twitter.summingbird.javaapi;

/**
 * A function that takes one parameter and returns a boolean meant to use in filter(p)
 * lambdas in java 8 can be passed to methods taking an interface defining only one method.
 *
 * @author Julien Le Dem
 *
 * @param <T> return type
 */
public interface Predicate<T> {
  boolean test(T v);
}
