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

package com.twitter.summingbird.graph

/////////////////////
// There is no logical reason for Literal[T, N] to be here,
// but the scala compiler crashes in 2.9.3 if it is not.
// with:
// java.lang.Error: typeConstructor inapplicable for <none>
//   at scala.tools.nsc.symtab.SymbolTable.abort(SymbolTable.scala:34)
//   at scala.tools.nsc.symtab.Symbols$Symbol.typeConstructor(Symbols.scala:880)
////////////////////

/**
 * This represents literal expressions (no variable redirection)
 * of container nodes of type N[T]
 */
sealed trait Literal[T, N[_]] {
  def evaluate: N[T] = Literal.evaluate(this)
}
case class ConstLit[T, N[_]](override val evaluate: N[T]) extends Literal[T, N]
case class UnaryLit[T1, T2, N[_]](arg: Literal[T1, N],
    fn: N[T1] => N[T2]) extends Literal[T2, N] {
}
case class BinaryLit[T1, T2, T3, N[_]](arg1: Literal[T1, N], arg2: Literal[T2, N],
    fn: (N[T1], N[T2]) => N[T3]) extends Literal[T3, N] {
}

object Literal {
  /**
   * This evaluates a literal formula back to what it represents
   * being careful to handle diamonds by creating referentially
   * equivalent structures (not just structurally equivalent)
   */
  def evaluate[T, N[_]](lit: Literal[T, N]): N[T] =
    evaluate(HMap.empty[({ type L[T] = Literal[T, N] })#L, N], lit)._2

  // Memoized version of the above to handle diamonds
  protected def evaluate[T, N[_]](hm: HMap[({ type L[T] = Literal[T, N] })#L, N], lit: Literal[T, N]): (HMap[({ type L[T] = Literal[T, N] })#L, N], N[T]) =
    hm.get(lit) match {
      case Some(prod) => (hm, prod)
      case None =>
        lit match {
          case ConstLit(prod) => (hm + (lit -> prod), prod)
          case UnaryLit(in, fn) =>
            val (h1, p1) = evaluate(hm, in)
            val p2 = fn(p1)
            (h1 + (lit -> p2), p2)
          case BinaryLit(in1, in2, fn) =>
            val (h1, p1) = evaluate(hm, in1)
            val (h2, p2) = evaluate(h1, in2)
            val p3 = fn(p1, p2)
            (h2 + (lit -> p3), p3)
        }
    }
}

trait ExpressionDag[N[_]] { self =>
  // Once we fix N above, we can make E[T] = Expr[T, N]
  type E[t] = Expr[t, N]
  def idToExp: HMap[Id, E]
  protected def nextId: Int

  override def toString: String =
    "ExpressionDag(idToExp = %s)".format(idToExp)

  // This is a cache of Id[T] => Option[N[T]]
  private val idToN =
    new HCache[Id, ({ type ON[T] = Option[N[T]] })#ON]()

  // This is a cache of Node[T] => Id[T]
  private val nodeToId = new HCache[N, ({ type OI[T] = Option[Id[T]] })#OI]()

  /**
   * Apply the given rule to the given dag until
   * the graph no longer changes.
   */
  def apply(rule: Rule[N]): ExpressionDag[N] = {
    // for some reason, scala can't optimize this with tailrec
    var prev: ExpressionDag[N] = null
    var curr: ExpressionDag[N] = this
    while (!(curr eq prev)) {
      prev = curr
      curr = curr.applyOnce(rule)
    }
    curr
  }

  /**
   * apply the rule at the first place that satisfies
   * it, and return from there.
   */
  def applyOnce(rule: Rule[N]): ExpressionDag[N] = {
    val getP = new GenPartial[Id, E] {
      def apply[U] = {
        val fn = rule.apply[U](self)

        { case x if fn(x).isDefined => fn(x).get }
      }
    }
    idToExp.updateFirst(getP) match {
      case None => this
      case Some((newIdExp, xid)) =>
        // the type below helps the compiler
        def act[T](id: Id[T]) = {
          val newN: N[T] = newIdExp(id).evaluate(newIdExp)
          new ExpressionDag[N] {
            def idToExp = newIdExp
            def nextId = self.nextId
          }
        }
        // Run, and then apply the rule again
        act(xid).apply(rule)
    }
  }

  private def addExp[T](node: N[T], id: Id[T], exp: Expr[T, N]): ExpressionDag[N] =
    new ExpressionDag[N] {
      val idToExp = self.idToExp + (id -> exp)
      val nextId = self.nextId
    }

  private def assignId[T](l: Literal[T, N]): (ExpressionDag[N], Id[T]) = {
    val id0 = Id[T](nextId)
    (new ExpressionDag[N] {
      val idToExp = self.idToExp
      val nextId = self.nextId + 1
    }, id0)
  }

  /**
   * This finds some Id in the current graph that evaluates
   * to the given N[T]
   */
  def find[T](node: N[T]): Option[Id[T]] = nodeToId.getOrElseUpdate(node, {
    val partial = new GenPartial[HMap[Id, E]#Pair, Id] {
      def apply[T] = {
        case (id, e) if e.evaluate(idToExp) == node => id
      }
    }
    idToExp.collect(partial).headOption
      .asInstanceOf[Option[Id[T]]]
  })

  /**
   * This throws if the node is missing, use find if this is not
   * a logic error in your programming. With dependent types we could
   * possibly get this to not compile if it could throw.
   */
  def idOf[T](node: N[T]): Id[T] =
    find(node).getOrElse(sys.error("could not get node: %s\n from %s".format(node, this)))

  /**
   * ensure the given literal node is present in the Dag
   */
  protected def ensure[T](lit: Literal[T, N]): (ExpressionDag[N], Id[T]) = {
    val node = lit.evaluate
    find(node) match {
      case Some(id) => (this, id)
      case None =>
        lit match {
          case ConstLit(n) =>
            val (exp1, id) = assignId(lit)
            (exp1.addExp(node, id, Const(n)), id)
          case UnaryLit(prev, fn) =>
            val (exp1, idprev) = ensure(prev)
            val (exp2, id) = exp1.assignId(lit)
            (exp2.addExp(node, id, Unary(idprev, fn)), id)
          case BinaryLit(n1, n2, fn) =>
            val (exp1, id1) = ensure(n1)
            val (exp2, id2) = exp1.ensure(n1)
            val (exp3, id) = exp2.assignId(lit)
            (exp3.addExp(node, id, Binary(id1, id2, fn)), id)
        }
    }
  }

  /**
   * After applying rules to your Dag, use this method
   * to get the original node type
   */
  def evaluate[T](id: Id[T]): Option[N[T]] =
    idToN.getOrElseUpdate(id, {
      idToExp.get(id).map { exp =>
        exp.evaluate(idToExp)
      }
    })

  /**
   * Return the number of nodes that depend on the
   * given Id, TODO we might want to cache these.
   */
  def fanOut(id: Id[_]): Int = {
    // We make a fake IntT[T] which is just Int
    val partial = new GenPartial[E, ({ type IntT[T] = Int })#IntT] {
      def apply[T] = {
        case Var(id1) if (id1 == id) => 1
        case Unary(id1, fn) if (id1 == id) => 1
        case Binary(id1, id2, fn) if (id1 == id) && (id2 == id) => 2
        case Binary(id1, id2, fn) if (id1 == id) || (id2 == id) => 1
        case _ => 0
      }
    }
    idToExp.collectValues[({ type IntT[T] = Int })#IntT](partial).sum
  }

  def fanOut(node: N[_]): Int = fanOut(idOf(node))
}

object ExpressionDag {
  private def empty[N[_]]: ExpressionDag[N] = new ExpressionDag[N] {
    def idToExp = HMap.empty[Id, ({ type E[t] = Expr[t, N] })#E]
    def nextId = 0
  }
  def apply[T, N[_]](tail: Literal[T, N]): (ExpressionDag[N], Id[T]) =
    empty.ensure(tail)
}

/**
 * This implements a simplification rule on ExpressionDags
 */
trait Rule[N[_]] {
  /**
   * If the given Id can be replaced with a simpler expression,
   * return Some(expr) else None.
   *
   * If it is convenient, you might write a partial function
   * and then call .lift to get the correct Function type
   */
  def apply[T](on: ExpressionDag[N]): (Id[T] => Option[Expr[T, N]])

  // // If the current rule cannot apply, then try the argument here
  // def orElse(that: Rule[N]): Rule[N] = new Rule[N] {
  //   def apply[T](on: ExpressionDag[N]) = { id =>
  //     self(on)(id).orElse(that(on)(id))
  //   }
  // }
}

