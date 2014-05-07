package com.twitter.summingbird.memory.javaapi;

public interface JSink<IN> {
  void write(IN p);
}
