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
    def assertInitName(n: Producer[Memory, Any], s: List[String]) =
      assert(deps.namesOf(n).map(_.id) == s)

    assertInitName(src0, List("source0", "map0", "sumByKey"))
    assertInitName(src1, List("source1", "map1", "sumByKey"))
    assertInitName(mapped0, List("map0", "sumByKey"))
    assertInitName(mapped1, List("map1", "sumByKey"))
    assertInitName(summed, List("sumByKey"))

    val (nameMap, stripped) = StripNamedNode(graph)
    val strippedDeps = Dependants(stripped)

    def assertName(names: List[String])(p: PartialFunction[Producer[Memory, Any], Producer[Memory, Any]]): Unit = {
      val nodes = strippedDeps.nodes.collect(p)
      assert(nodes.size == 1) // Only one node
      assert(nameMap(nodes(0)) == names, s"checking ${names}")
    }

    assertName(List("source0", "map0", "sumByKey")) { case p @ Source(l) if l == input0 => p }
    assertName(List("map0", "sumByKey")) { case p @ OptionMappedProducer(_, f) if f == fn0 => p }
    assertName(List("source1", "map1", "sumByKey")) { case p @ Source(l) if l == input1 => p }
    assertName(List("map1", "sumByKey")) { case p @ OptionMappedProducer(_, f) if f == fn1 => p }
    assertName(List("sumByKey")) { case p @ Summer(_, str, _) if str == store => p }

    // The final stripped has no names:
    assert(strippedDeps.nodes.collect { case NamedProducer(_, _) => 1 }.sum == 0)
  }

  test("Fan-out name test") {
    /*
     * Here are the irreducible items
     */
    val store0 = MMap[Int, Int]()
    val store1 = MMap[Int, Int]()
    val input = List(1, 2, 4)
    val fn0 = { k: Int => Some((k % 2, k * k)) }
    val fn1 = { k: Int => Some((k % 3, k * k * k)) }
    // Here is the graph
    val src = Producer.source[Memory, Int](input)
    val nameSrc = src.name("source")
    // branch1
    val mapped0 = nameSrc.optionMap(fn0)
    val summed0 = mapped0.name("map0").sumByKey(store0)
    // branch2
    val mapped1 = nameSrc.optionMap(fn1)
    val summed1 = mapped1.name("map1").name("map1.1").sumByKey(store1)
    val graph = summed0.name("sumByKey0").also(summed1.name("sumByKey1"))
    val namedG = graph.name("also")
    val deps = Dependants(namedG)
    /*
     * With fan-out, a total order on the lists is not defined.
     * so we check that the given list is in sorted order where
     * the partial ordering is defined.
     */
    def assertInitName(n: Producer[Memory, Any], s: List[String]) = {
      val ordering = deps.namesOf(n).map(_.id).zipWithIndex.toMap
      val order = Ordering.by(ordering)
      assert(s.sorted(order) == s, s"not sorted: $s != ${s.sorted(order)}")
    }

    assertInitName(src, List("source", "map0", "sumByKey0"))
    assertInitName(src, List("source", "map1", "sumByKey1", "also"))
    assertInitName(src, List("source", "map1", "map1.1", "sumByKey1", "also"))
    assertInitName(src, List("also"))
    // the "also" name only goes up the right hand side
    // because the output of the also only depends on the right hand side
    assertInitName(mapped0, List("map0", "sumByKey0"))
    assertInitName(mapped1, List("map1", "map1.1", "sumByKey1", "also"))
    assertInitName(summed0, List("sumByKey0"))
    assertInitName(summed1, List("sumByKey1", "also"))
    assertInitName(graph, List("also"))

    val (nameMap, stripped) = StripNamedNode(namedG)
    val strippedDeps = Dependants(stripped)

    def assertName(names: List[String])(p: PartialFunction[Producer[Memory, Any], Producer[Memory, Any]]): Unit = {
      val nodes = strippedDeps.nodes.collect(p)
      assert(nodes.size == 1) // Only one node
      val ordering = nameMap(nodes(0)).zipWithIndex.toMap
      val order = Ordering.by(ordering)
      assert(names.sorted(order) == names, s"not sorted: $names != ${names.sorted(order)}")
    }
    assertName(List("source", "map0", "sumByKey0")) { case p @ Source(l) if l == input => p }
    assertName(List("source", "map1", "map1.1", "sumByKey1", "also")) { case p @ Source(l) if l == input => p }
    assertName(List("map0", "sumByKey0")) { case p @ OptionMappedProducer(_, f) if f == fn0 => p }
    assertName(List("map1", "sumByKey1", "also")) { case p @ OptionMappedProducer(_, f) if f == fn1 => p }
    assertName(List("sumByKey0")) { case p @ Summer(_, str, _) if str eq store0 => p }
    assertName(List("sumByKey1", "also")) { case p @ Summer(_, str, _) if str eq store1 => p }
    // The final stripped has no names:
    assert(strippedDeps.nodes.collect { case NamedProducer(_, _) => 1 }.sum == 0)
  }
}
