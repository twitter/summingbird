package com.twitter.summingbird.memory.javaapi;

/**
 *
 * A function that takes one parameter and does not return anything.
 * lambdas in java 8 can be passed to methods taking an interface defining only one method.
 * this is easier to use than Function<IN, Void> as it does not require <code>return null;</code> to compile
 *
 * @author Julien Le Dem
 *
 * @param <IN> parameter
 */
public interface JSink<IN> {
  void write(IN p);
}
