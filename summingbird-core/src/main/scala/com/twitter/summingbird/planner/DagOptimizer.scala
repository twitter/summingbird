/*
 Copyright 2014 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.summingbird.planner

import com.twitter.summingbird._
import com.twitter.summingbird.graph._
import com.twitter.algebird.Semigroup

trait DagOptimizer[P <: Platform[P]] {

  type Prod[T] = Producer[P, T]

  /**
   * This makes a potentially unsound cast. Since this method is only use
   * in converting from an AlsoProducer to a Literal[T, Prod] below, it is
   * not actually dangerous because we always use it in a safe position.
   */
  protected def mkAlso[T, U]: (Prod[T], Prod[U]) => Prod[U] = {
    (left, right) => AlsoProducer(left.asInstanceOf[TailProducer[P, T]], right)
  }
  protected def mkAlsoTail[T, U]: (Prod[T], Prod[U]) => Prod[U] = {
    (left, right) => new AlsoTailProducer(left.asInstanceOf[TailProducer[P, T]], right.asInstanceOf[TailProducer[P, U]])
  }
  protected def mkMerge[T]: (Prod[T], Prod[T]) => Prod[T] = {
    (left, right) => MergedProducer(left, right)
  }
  protected def mkNamed[T](name: String): (Prod[T] => Prod[T]) = {
    prod => NamedProducer(prod, name)
  }
  protected def mkTPNamed[T](name: String): (Prod[T] => Prod[T]) = {
    prod => new TPNamedProducer(prod.asInstanceOf[TailProducer[P, T]], name)
  }
  protected def mkIdentKey[K, V]: (Prod[(K, V)] => Prod[(K, V)]) = {
    prod => IdentityKeyedProducer(prod)
  }
  protected def mkOptMap[T, U](fn: T => Option[U]): (Prod[T] => Prod[U]) = {
    prod => OptionMappedProducer(prod, fn)
  }
  protected def mkFlatMapped[T, U](fn: T => TraversableOnce[U]): (Prod[T] => Prod[U]) = {
    prod => FlatMappedProducer(prod, fn)
  }
  protected def mkKeyFM[T, U, V](fn: T => TraversableOnce[U]): (Prod[(T, V)] => Prod[(U, V)]) = {
    prod => KeyFlatMappedProducer(prod, fn)
  }
  protected def mkValueFM[K, U, V](fn: U => TraversableOnce[V]): (Prod[(K, U)] => Prod[(K, V)]) = {
    prod => ValueFlatMappedProducer(prod, fn)
  }
  protected def mkWritten[T, U >: T](sink: P#Sink[U]): (Prod[T] => Prod[T]) = {
    prod => WrittenProducer[P, T, U](prod, sink)
  }
  protected def mkSrv[K, T, V](serv: P#Service[K, V]): (Prod[(K, T)] => Prod[(K, (T, Option[V]))]) = {
    prod => LeftJoinedProducer(prod, serv)
  }
  protected def mkSum[K, V](store: P#Store[K, V], sg: Semigroup[V]): (Prod[(K, V)] => Prod[(K, (Option[V], V))]) = {
    prod => Summer(prod, store, sg)
  }

  type LitProd[T] = Literal[T, Prod]

  /**
   * Convert a Producer graph into a Literal in the Dag rewriter
   * This is where the tedious work comes in.
   */
  def toLiteral[T](prod: Producer[P, T]): Literal[T, Prod] =
    toLiteral(HMap.empty[Prod, LitProd], prod)._2

  protected def toLiteral[T](hm: HMap[Prod, LitProd], prod: Producer[P, T]): (HMap[Prod, LitProd], LitProd[T]) = {
    // These get typed over and over below
    type N[t] = Prod[t]
    type M = HMap[Prod, LitProd]
    type L[t] = Literal[t, N]

    /**
     * All this shit is due to the scala compiler's inability to see the types
     * in case matches. I can see this is unneeded, why can't scala?
     */

    def source[T1 <: T](t: Source[P, T1]): (M, L[T]) = {
      val lit = ConstLit[T, N](t)
      (hm + (t -> lit), lit)
    }
    def also[R](a: AlsoProducer[P, R, T]): (M, L[T]) = {
      val (h1, l1) = toLiteral(hm, a.ensure)
      val (h2, l2) = toLiteral(h1, a.result)
      val lit = BinaryLit[R, T, T, N](l1, l2, mkAlso)
      (h2 + (a -> lit), lit)
    }
    def alsoTail[R](a: AlsoTailProducer[P, R, T]): (M, L[T]) = {
      val (h1, l1) = toLiteral(hm, a.ensure)
      val (h2, l2) = toLiteral(h1, a.result)
      val lit = BinaryLit[R, T, T, N](l1, l2, mkAlsoTail)
      (h2 + (a -> lit), lit)
    }
    def merge(m: MergedProducer[P, T]): (M, L[T]) = {
      val (h1, l1) = toLiteral(hm, m.left)
      val (h2, l2) = toLiteral(h1, m.right)
      val lit = BinaryLit[T, T, T, N](l1, l2, mkMerge)
      (h2 + (m -> lit), lit)
    }
    def named(n: NamedProducer[P, T]): (M, L[T]) = {
      val (h1, l1) = toLiteral(hm, n.producer)
      val lit = UnaryLit[T, T, N](l1, mkNamed(n.id))
      (h1 + (n -> lit), lit)
    }
    def namedTP(n: TPNamedProducer[P, T]): (M, L[T]) = {
      val (h1, l1) = toLiteral(hm, n.producer)
      val lit = UnaryLit[T, T, N](l1, mkTPNamed(n.id))
      (h1 + (n -> lit), lit)
    }
    def ikp[K, V](ik: IdentityKeyedProducer[P, K, V]): (M, L[(K, V)]) = {
      val (h1, l1) = toLiteral(hm, ik.producer)
      val lit = UnaryLit[(K, V), (K, V), N](l1, mkIdentKey)
      (h1 + (ik -> lit), lit)
    }
    def optm[T1](optm: OptionMappedProducer[P, T1, T]): (M, L[T]) = {
      val (h1, l1) = toLiteral(hm, optm.producer)
      val lit = UnaryLit[T1, T, N](l1, mkOptMap(optm.fn))
      (h1 + (optm -> lit), lit)
    }
    def flm[T1](fm: FlatMappedProducer[P, T1, T]): (M, L[T]) = {
      val (h1, l1) = toLiteral(hm, fm.producer)
      val lit = UnaryLit[T1, T, N](l1, mkFlatMapped(fm.fn))
      (h1 + (fm -> lit), lit)
    }
    def kfm[K, V, K2](kf: KeyFlatMappedProducer[P, K, V, K2]): (M, L[(K2, V)]) = {
      val (h1, l1) = toLiteral(hm, kf.producer)
      val lit = UnaryLit[(K, V), (K2, V), N](l1, mkKeyFM(kf.fn))
      (h1 + (kf -> lit), lit)
    }
    def vfm[K, V, V2](kf: ValueFlatMappedProducer[P, K, V, V2]): (M, L[(K, V2)]) = {
      val (h1, l1) = toLiteral(hm, kf.producer)
      val lit = UnaryLit[(K, V), (K, V2), N](l1, mkValueFM(kf.fn))
      (h1 + (kf -> lit), lit)
    }
    def writer[T1 <: T, U >: T1](w: WrittenProducer[P, T1, U]): (M, L[T]) = {
      val (h1, l1) = toLiteral(hm, w.producer)
      val lit = UnaryLit[T1, T, N](l1, mkWritten[T1, U](w.sink))
      (h1 + (w -> lit), lit)
    }
    def joined[K, V, U](join: LeftJoinedProducer[P, K, V, U]): (M, L[(K, (V, Option[U]))]) = {
      val (h1, l1) = toLiteral(hm, join.left)
      val lit = UnaryLit[(K, V), (K, (V, Option[U])), N](l1, mkSrv(join.joined))
      (h1 + (join -> lit), lit)
    }
    def summer[K, V](s: Summer[P, K, V]): (M, L[(K, (Option[V], V))]) = {
      val (h1, l1) = toLiteral(hm, s.producer)
      val lit = UnaryLit[(K, V), (K, (Option[V], V)), N](l1, mkSum(s.store, s.semigroup))
      (h1 + (s -> lit), lit)
    }

    // the keyed have to be cast because
    // all the keyed get inferred types (Any, Any), not
    // (K, V) <: T, which is what they are
    def cast[K, V](tup: (M, L[(K, V)])): (M, L[T]) =
      tup.asInstanceOf[(M, L[T])]

    hm.get(prod) match {
      case Some(lit) => (hm, lit)
      case None =>
        prod match {
          case s @ Source(_) => source(s)
          case a: AlsoTailProducer[_, _, _] => alsoTail(a.asInstanceOf[AlsoTailProducer[P, _, T]])
          case a @ AlsoProducer(_, _) => also(a)
          case m @ MergedProducer(l, r) => merge(m)
          case n: TPNamedProducer[_, _] => namedTP(n.asInstanceOf[TPNamedProducer[P, T]])
          case n @ NamedProducer(producer, name) => named(n)
          case w @ WrittenProducer(producer, sink) => writer(w)
          case fm @ FlatMappedProducer(producer, fn) => flm(fm)
          case om @ OptionMappedProducer(producer, fn) => optm(om)
          // These casts can't fail due to the pattern match,
          // but I can't convince scala of this without the cast.
          case ik @ IdentityKeyedProducer(producer) => cast(ikp(ik))
          case kf @ KeyFlatMappedProducer(producer, fn) => cast(kfm(kf))
          case vf @ ValueFlatMappedProducer(producer, fn) => cast(vfm(vf))
          case j @ LeftJoinedProducer(producer, srv) => cast(joined(j))
          case s @ Summer(producer, store, sg) => cast(summer(s))
        }
    }
  }

  /**
   * Create an ExpressionDag for the given node. This should be the
   * final tail of the graph. You can apply optimizations on this
   * Dag and then use the Id returned to evaluate it back to an
   * optimized producer
   */
  def expressionDag[T](p: Producer[P, T]): (ExpressionDag[Prod], Id[T]) = {
    val prodToLit = new GenFunction[Prod, LitProd] {
      def apply[T] = { p => toLiteral(p) }
    }
    ExpressionDag[T, Prod](p, prodToLit)
  }

  /**
   * Optimize the given producer according to the rule
   */
  def optimize[T](p: Producer[P, T], rule: Rule[Prod]): Producer[P, T] = {
    val (dag, id) = expressionDag(p)
    dag(rule).evaluate(id)
  }

  /*
   * Here are some generic optimization rules that might apply to all
   * graphs regardless of underlying platform
   */

  /**
   * Strip all the names. Names are rightly considered as names on the irreducible
   * parts of the input graph (functions, stores, sinks, sources, etc...) and not
   * the AST that we generate and optimize along the way
   */
  object RemoveNames extends PartialRule[Prod] {
    def applyWhere[T](on: ExpressionDag[Prod]) = {
      case NamedProducer(p, _) => p
    }
  }

  /**
   * Identity keyed producer is just a trick to make scala see methods on keyed
   * types, they have no meaning at runtime.
   */
  object RemoveIdentityKeyed extends PartialRule[Prod] {
    def applyWhere[T](on: ExpressionDag[Prod]) = {
      // scala can't see that (K, V) <: T
      case IdentityKeyedProducer(p) => p.asInstanceOf[Prod[T]]
    }
  }

  /**
   * a.flatMap(fn).flatMap(fn2) can be written as a.flatMap(compose(fn, fn2))
   */
  object FlatMapFusion extends PartialRule[Prod] {
    def applyWhere[T](on: ExpressionDag[Prod]) = {
      //Can't fuse flatMaps when on fanout
      case FlatMappedProducer(in1 @ FlatMappedProducer(in0, fn0), fn1) if (on.fanOut(in1) == 1) =>
        FlatMappedProducer(in0, ComposedFlatMap(fn0, fn1))
    }
  }

  // a.optionMap(b).optionMap(c) == a.optionMap(compose(b, c))
  object OptionMapFusion extends PartialRule[Prod] {
    def applyWhere[T](on: ExpressionDag[Prod]) = {
      //Can't fuse flatMaps when on fanout
      case OptionMappedProducer(in1 @ OptionMappedProducer(in0, fn0), fn1) if (on.fanOut(in1) == 1) =>
        OptionMappedProducer(in0, ComposedOptionMap(fn0, fn1))
    }
  }

  /**
   * If you don't care to distinguish between optionMap and flatMap,
   * you can use this rule
   */
  object OptionToFlatMap extends PartialRule[Prod] {
    def applyWhere[T](on: ExpressionDag[Prod]) = {
      //Can't fuse flatMaps when on fanout
      case OptionMappedProducer(in, fn) => in.flatMap(OptionToFlat(fn))
    }
  }
  /**
   * If you can't optimize KeyFlatMaps, use this
   */
  object KeyFlatMapToFlatMap extends PartialRule[Prod] {
    def applyWhere[T](on: ExpressionDag[Prod]) = {
      //Can't fuse flatMaps when on fanout
      // TODO: we need to case class here to not lose the irreducible which may be named
      case KeyFlatMappedProducer(in, fn) =>
        // we know that (K, V) <: T due to the case match, but scala can't see it
        def cast[K, V](p: Prod[(K, V)]): Prod[T] = p.asInstanceOf[Prod[T]]
        cast(in.flatMap { case (k, v) => fn(k).map((_, v)) })
    }
  }
  /**
   * If you can't optimize ValueFlatMaps, use this
   */
  object ValueFlatMapToFlatMap extends PartialRule[Prod] {
    def applyWhere[T](on: ExpressionDag[Prod]) = {
      // TODO: we need to case class here to not lose the irreducible which may be named
      case ValueFlatMappedProducer(in, fn) =>
        // we know that (K, V) <: T due to the case match, but scala can't see it
        def cast[K, V](p: Prod[(K, V)]): Prod[T] = p.asInstanceOf[Prod[T]]
        cast(in.flatMap { case (k, v) => fn(v).map((k, _)) })
    }
  }

  /**
   * Combine flatMaps followed by optionMap into a single operation
   *
   * On the other direction, you might not want to run optionMap with flatMap since some
   * platforms (storm) can't easily control source parallelism, so we don't want to push
   * big expansions up to sources
   */
  object FlatThenOptionFusion extends PartialRule[Prod] {
    def applyWhere[T](on: ExpressionDag[Prod]) = {
      //Can't fuse flatMaps when on fanout
      case OptionMappedProducer(in1 @ FlatMappedProducer(in0, fn0), fn1) if (on.fanOut(in1) == 1) =>
        FlatMappedProducer(in0, ComposedFlatMap(fn0, OptionToFlat(fn1)))
    }
  }

  /**
   * (a.flatMap(f1) ++ a.flatMap(f2)) == a.flatMap { i => f1(i) ++ f2(i) }
   */
  object DiamondToFlatMap extends PartialRule[Prod] {
    def applyWhere[T](on: ExpressionDag[Prod]) = {
      //Can't fuse flatMaps when on fanout
      case MergedProducer(left @ FlatMappedProducer(inleft, fnleft),
        right @ FlatMappedProducer(inright, fnright)) if (inleft == inright) && (on.fanOut(left) == 1) && (on.fanOut(right) == 1) =>
        FlatMappedProducer(inleft, MergeResults(fnleft, fnright))
    }
  }

  /**
   * (a ++ b).flatMap(fn) == (a.flatMap(fn) ++ b.flatMap(fn))
   * (a ++ b).optionMap(fn) == (a.optionMap(fn) ++ b.optionMap(fn))
   * and since Merge is usually a no-op when combined with a grouping operation, it
   * often pays to get merges as high the graph as possible.
   */
  object MergePullUp extends PartialRule[Prod] {
    //Can't do this operation if the merge fans out
    def applyWhere[T](on: ExpressionDag[Prod]) = {
      case OptionMappedProducer(m @ MergedProducer(a, b), fn) if (on.fanOut(m) == 1) =>
        (a.optionMap(fn)) ++ (b.optionMap(fn))
      case FlatMappedProducer(m @ MergedProducer(a, b), fn) if (on.fanOut(m) == 1) =>
        (a.flatMap(fn)) ++ (b.flatMap(fn))
    }
  }

  /**
   * We can always push all Also nodes all the way to the bottom of the dag
   * MergedProducer(AlsoProducer(t, a), b) == AlsoProducer(t, MergedProducer(a, b))
   *
   * Unary(l, fn), if l == AlsoProducer(tail, r) can be changed to
   *   AlsoProducer(tail, fn(r))
   */
  object AlsoPullUp extends Rule[Prod] {
    def apply[T](on: ExpressionDag[Prod]) = {
      case a @ AlsoProducer(_, _) => None // If we are already an also, we are done
      case MergedProducer(AlsoProducer(tail, l), r) =>
        Some(AlsoProducer(tail, l ++ r))
      case MergedProducer(l, AlsoProducer(tail, r)) =>
        Some(AlsoProducer(tail, l ++ r))
      case node => on.toLiteral(node) match {
        // There are a lot of unary operators, use the literal graph here:
        // note that this cannot be an Also, due to the first case
        case UnaryLit(alsoLit, fn) =>
          alsoLit.evaluate match {
            case AlsoProducer(tail, rest) =>
              fn(rest) match {
                case rightTail: TailProducer[_, _] =>
                  // The type of the result must be T, but scala
                  // can't see this
                  val typedTail = rightTail.asInstanceOf[TailProducer[P, T]]
                  Some(new AlsoTailProducer(tail, typedTail))
                case nonTail => Some(AlsoProducer(tail, nonTail))
              }
            case _ => None
          }
        case _ => None
      }
    }
  }
}

