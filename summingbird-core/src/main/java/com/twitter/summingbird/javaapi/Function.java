package com.twitter.summingbird.javaapi;

public interface Function<IN, OUT> {
  OUT apply(IN p);
}
