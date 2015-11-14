package com.twitter.summingbird.online

import com.twitter.summingbird.memory.Memory
import com.twitter.summingbird.planner.StripNamedNode
import com.twitter.summingbird.{ Dependants, Producer, OptionMappedProducer, NamedProducer, Source, Summer }
import org.scalatest.FunSuite
import scala.collection.mutable.{ Map => MMap }

class StripNameTest extends FunSuite {

  test("simple name test") {
    /*
     * Here are the irreducible items
     */
    val store = MMap[Int, Int]()
    val input = List(1, 2, 4)
    val fn = { k: Int => Some((k % 2, k * k)) }

    val src = Producer.source[Memory, Int](input)
    val mapped = src
      .name("source")
      .optionMap(fn)
    val summed = mapped
      .name("map")
      .sumByKey(store)
    val graph = summed
      .name("sumByKey")

    val deps = Dependants(graph)
    assert(deps.namesOf(src).map(_.id).toSet == Set("source", "map", "sumByKey"))
    assert(deps.namesOf(mapped).map(_.id).toSet == Set("map", "sumByKey"))
    assert(deps.namesOf(summed).map(_.id).toSet == Set("sumByKey"))

    val (nameMap, stripped) = StripNamedNode(graph)
    val strippedDeps = Dependants(stripped)

    def assertName(names: Set[String])(p: PartialFunction[Producer[Memory, Any], Producer[Memory, Any]]): Unit = {
      val nodes = strippedDeps.nodes.collect(p)
      assert(nodes.size == 1) // Only one node
      assert(nameMap(nodes(0)).toSet == names, s"checking ${names}")
    }

    assertName(Set("source", "map", "sumByKey")) { case p @ Source(l) if l == input => p }
    assertName(Set("map", "sumByKey")) { case p @ OptionMappedProducer(_, f) if f == fn => p }
    assertName(Set("sumByKey")) { case p @ Summer(_, str, _) if str == store => p }

    // The final stripped has no names:
    assert(strippedDeps.nodes.collect { case NamedProducer(_, _) => 1 }.sum == 0)
  }
  test("merge name test") {
    /*
     * Here are the irreducible items
     */
    val store = MMap[Int, Int]()
    val input0 = List(1, 2, 4)
    val input1 = List("100", "200", "400")
    val fn0 = { k: Int => Some((k % 2, k * k)) }
    val fn1 = { kstr: String => val k = kstr.toInt; Some((k % 2, k * k)) }

    val src0 = Producer.source[Memory, Int](input0)
    val mapped0 = src0
      .name("source0")
      .optionMap(fn0)
    val named0 = mapped0.name("map0")

    val src1 = Producer.source[Memory, String](input1)
    val mapped1 = src1
      .name("source1")
      .optionMap(fn1)
    val named1 = mapped1.name("map1")

    val summed = (named0 ++ named1).sumByKey(store)
    val graph = summed
      .name("sumByKey")

    val deps = Dependants(graph)
    def assertInitName(n: Producer[Memory, Any], s: Set[String]) =
      assert(deps.namesOf(n).map(_.id).toSet == s)

    assertInitName(src0, Set("source0", "map0", "sumByKey"))
    assertInitName(src1, Set("source1", "map1", "sumByKey"))
    assertInitName(mapped0, Set("map0", "sumByKey"))
    assertInitName(mapped1, Set("map1", "sumByKey"))
    assertInitName(summed, Set("sumByKey"))

    val (nameMap, stripped) = StripNamedNode(graph)
    val strippedDeps = Dependants(stripped)

    def assertName(names: Set[String])(p: PartialFunction[Producer[Memory, Any], Producer[Memory, Any]]): Unit = {
      val nodes = strippedDeps.nodes.collect(p)
      assert(nodes.size == 1) // Only one node
      assert(nameMap(nodes(0)).toSet == names, s"checking ${names}")
    }

    assertName(Set("source0", "map0", "sumByKey")) { case p @ Source(l) if l == input0 => p }
    assertName(Set("map0", "sumByKey")) { case p @ OptionMappedProducer(_, f) if f == fn0 => p }
    assertName(Set("source1", "map1", "sumByKey")) { case p @ Source(l) if l == input1 => p }
    assertName(Set("map1", "sumByKey")) { case p @ OptionMappedProducer(_, f) if f == fn1 => p }
    assertName(Set("sumByKey")) { case p @ Summer(_, str, _) if str == store => p }

    // The final stripped has no names:
    assert(strippedDeps.nodes.collect { case NamedProducer(_, _) => 1 }.sum == 0)
  }
}
