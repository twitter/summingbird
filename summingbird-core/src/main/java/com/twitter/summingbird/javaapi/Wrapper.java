package com.twitter.summingbird.javaapi;

public class Wrapper<T> {
  private final T wrapped;

  public T unwrap() {
    return wrapped;
  }

  public Wrapper(T wrapped) {
    this.wrapped = wrapped;
  }
}
