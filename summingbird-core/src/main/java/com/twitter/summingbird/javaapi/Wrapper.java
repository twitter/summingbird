package com.twitter.summingbird.javaapi;

/**
 * wraps a scala type to replace the path dependent types in Platform by a concrete type containing the same information
 *
 * @author Julien Le Dem
 *
 * @param <T> the wrapped type
 */
public class Wrapper<T> {
  private final T wrapped;

  public T unwrap() {
    return wrapped;
  }

  public Wrapper(T wrapped) {
    this.wrapped = wrapped;
  }
}
