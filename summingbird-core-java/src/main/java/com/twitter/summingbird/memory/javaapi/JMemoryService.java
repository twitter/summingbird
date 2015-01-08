package com.twitter.summingbird.memory.javaapi;

import com.twitter.summingbird.memory.MemoryService;
import java.util.Map;
import scala.Option;

public class JMemoryService<K, V> implements MemoryService<K, V>  {

  private Map<K, V> serviceMap;

  public JMemoryService(Map<K, V> m) {
    serviceMap = m;
  }

  public Option<V> get(K key) {
    return Option.apply(serviceMap.get(key));
  }
}
