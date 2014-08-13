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

  def toProducer[T](lit: Literal[T, Prod]): Producer[P, T] = lit.evaluate

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
    def apply[T](on: ExpressionDag[Prod]): (Id[T] => Option[Expr[T, Prod]]) = { id =>
      on.evaluate(id) match {
        case Some(NamedProducer(p, _)) => Some(Var(on.idOf(p)))
        case _ => None
      }
    }
  }

  /**
   * Identity keyed producer is just a trick to make scala see methods on keyed
   * types, they have no meaning at runtime.
   */
  object RemoveIdentityKeyed extends Rule[Prod] {
    def apply[T](on: ExpressionDag[Prod]) = { id =>
      on.evaluate(id) match {
        case Some(IdentityKeyedProducer(p)) =>
          val idOf = on.idOf(p)
            .asInstanceOf[Id[T]] // The standard scala inability to see that T must be (K, V) here
          Some(Var(idOf))
        case _ => None
      }
    }
  }

  /**
   * a.flatMap(fn).flatMap(fn2) can be written as a.flatMap(compose(fn, fn2))
   */
  object FlatMapFusion extends Rule[Prod] {
    def apply[T](on: ExpressionDag[Prod]) = { id =>
      import on._
      evaluate(id) match {
        //Can't fuse flatMaps when on fanout
        case Some(FlatMappedProducer(in1 @ FlatMappedProducer(in0, fn0), fn1)) if (fanOut(in1) < 2) =>
          // TODO: There should probably be a case class here
          // to check for reference equality with named elements from the original graph
          def compose[T1, T2, T3](g: T1 => TraversableOnce[T2],
            h: T2 => TraversableOnce[T3]): T1 => TraversableOnce[T3] = { t1: T1 =>
            g(t1).flatMap(h)
          }
          // We have to jump through these type hoops to satisfy the compiler
          def flatMap[T1, T2, T3](g: T1 => TraversableOnce[T2],
            h: T2 => TraversableOnce[T3]): Prod[T1] => Prod[T3] = { in: Prod[T1] =>
            in.flatMap(compose(g, h))
          }
          Some(Unary(idOf(in0), flatMap(fn0, fn1)))
        case _ => None
      }
    }
  }
  /**
   * (a ++ b).flatMap(fn) == (a.flatMap(fn) ++ b.flatMap(fn))
   * and since Merge is usually a no-op when combined with a grouping operation, it
   * often pays to get merges as high the graph as possible
   */
  object MergePullUp extends Rule[Prod] {
    def apply[T](on: ExpressionDag[Prod]) = { id =>
      import on._
      evaluate(id) match {
        //Can't do this operation if the merge fans out
        case Some(FlatMappedProducer(m @ MergedProducer(a, b), fn)) if (fanOut(m) < 2) =>
          // We have to jump through these type hoops to satisfy the compiler
          def op[T1, T2](f: T1 => TraversableOnce[T2]): (Prod[T1], Prod[T1]) => Prod[T2] =
            { (left, right) => left.flatMap(f) ++ right.flatMap(f) }

          Some(Binary(idOf(a), idOf(b), op(fn)))
        case _ => None
      }
    }
  }
}

