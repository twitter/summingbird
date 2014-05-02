package com.twitter.summingbird.javaapi;

import scala.runtime.AbstractFunction1;

abstract public class Filter<T> extends AbstractFunction1<T, Boolean> {

  @Override
  public Boolean apply(T v) {
    return filter(v);
  }

  abstract public boolean filter(T v);

}
