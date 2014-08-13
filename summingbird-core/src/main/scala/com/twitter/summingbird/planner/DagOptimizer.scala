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
  protected def mkAlso[T, U]: (Producer[P, T], Producer[P, U]) => Producer[P, U] = {
    (left, right) => AlsoProducer(left.asInstanceOf[TailProducer[P, T]], right)
  }
  protected def mkMerge[T]: (Producer[P, T], Producer[P, T]) => Producer[P, T] = {
    (left, right) => MergedProducer(left, right)
  }
  protected def mkNamed[T](name: String): (Producer[P, T] => Producer[P, T]) = {
    prod => NamedProducer(prod, name)
  }
  protected def mkIdentKey[K, V]: (Producer[P, (K, V)] => Producer[P, (K, V)]) = {
    prod => IdentityKeyedProducer(prod)
  }
  protected def mkOptMap[T, U](fn: T => Option[U]): (Producer[P, T] => Producer[P, U]) = {
    prod => OptionMappedProducer(prod, fn)
  }
  protected def mkFlatMapped[T, U](fn: T => TraversableOnce[U]): (Producer[P, T] => Producer[P, U]) = {
    prod => FlatMappedProducer(prod, fn)
  }
  protected def mkKeyFM[T, U, V](fn: T => TraversableOnce[U]): (Producer[P, (T, V)] => Producer[P, (U, V)]) = {
    prod => KeyFlatMappedProducer(prod, fn)
  }
  protected def mkWritten[T, U >: T](sink: P#Sink[U]): (Producer[P, T] => Producer[P, T]) = {
    prod => WrittenProducer[P, T, U](prod, sink)
  }
  protected def mkSrv[K, T, V](serv: P#Service[K, V]): (Producer[P, (K, T)] => Producer[P, (K, (T, Option[V]))]) = {
    prod => LeftJoinedProducer(prod, serv)
  }
  protected def mkSum[K, V](store: P#Store[K, V], sg: Semigroup[V]): (Producer[P, (K, V)] => Producer[P, (K, (Option[V], V))]) = {
    prod => Summer(prod, store, sg)
  }

  type Prod[T] = Producer[P, T]
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
          case a @ AlsoProducer(_, _) => also(a)
          case m @ MergedProducer(l, r) => merge(m)
          case n @ NamedProducer(producer, name) => named(n)
          case w @ WrittenProducer(producer, sink) => writer(w)
          case fm @ FlatMappedProducer(producer, fn) => flm(fm)
          case om @ OptionMappedProducer(producer, fn) => optm(om)
          // These casts can't fail due to the pattern match,
          // but I can't convince scala of this without the cast.
          case ik @ IdentityKeyedProducer(producer) => cast(ikp(ik))
          case kf @ KeyFlatMappedProducer(producer, fn) => cast(kfm(kf))
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

  /*
   * Here are some generic optimization rules that might apply to all
   * graphs regardless of underlying platform
   */

  /**
   * Strip all the names. Names are rightly considered as names on the irreducible
   * parts of the input graph (functions, stores, sinks, sources, etc...) and not
   * the AST that we generate and optimize along the way
   */
  object RemoveNames extends Rule[Prod] {
    def apply[T](on: ExpressionDag[Prod]) = {
      case NamedProducer(p, _) => Some(p)
      case _ => None
    }
  }

  /**
   * Identity keyed producer is just a trick to make scala see methods on keyed
   * types, they have no meaning at runtime.
   */
  object RemoveIdentityKeyed extends Rule[Prod] {
    def apply[T](on: ExpressionDag[Prod]) = {
      // scala can't see that (K, V) <: T
      case IdentityKeyedProducer(p) => Some(p.asInstanceOf[Prod[T]])
      case _ => None
    }
  }

  /**
   * a.flatMap(fn).flatMap(fn2) can be written as a.flatMap(compose(fn, fn2))
   */
  object FlatMapFusion extends Rule[Prod] {
    def apply[T](on: ExpressionDag[Prod]) = {
      //Can't fuse flatMaps when on fanout
      case FlatMappedProducer(in1 @ FlatMappedProducer(in0, fn0), fn1) if (on.fanOut(in1) < 2) =>
        // TODO: There should probably be a case class here
        // to check for reference equality with named elements from the original graph
        def compose[T1, T2, T3](g: T1 => TraversableOnce[T2],
          h: T2 => TraversableOnce[T3]): T1 => TraversableOnce[T3] = { t1: T1 =>
          g(t1).flatMap(h)
        }
        Some(FlatMappedProducer(in0, compose(fn0, fn1)))
      case _ => None
    }
  }
  /**
   * (a ++ b).flatMap(fn) == (a.flatMap(fn) ++ b.flatMap(fn))
   * and since Merge is usually a no-op when combined with a grouping operation, it
   * often pays to get merges as high the graph as possible
   */
  object MergePullUp extends Rule[Prod] {
    def apply[T](on: ExpressionDag[Prod]) = {
      //Can't do this operation if the merge fans out
      case FlatMappedProducer(m @ MergedProducer(a, b), fn) if (on.fanOut(m) < 2) =>
        Some((a.flatMap(fn)) ++ (b.flatMap(fn)))
      case _ => None
    }
  }
}

