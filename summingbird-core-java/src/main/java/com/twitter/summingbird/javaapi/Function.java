package com.twitter.summingbird.javaapi;

/**
 *
 * A function that takes one parameter.
 * lambdas in java 8 can be passed to methods taking an interface defining only one method.
 *
 * @author Julien Le Dem
 *
 * @param <IN> parameter
 * @param <OUT> return type
 */
public interface Function<IN, OUT> {
  OUT apply(IN p);
}
