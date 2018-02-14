package com.twitter.summingbird.planner

import com.twitter.summingbird._
import com.twitter.algebird.Semigroup
import com.stripe.dagon.{Dag => DagonDag, _}
import java.io.Serializable

class DagOptimizer[P <: Platform[P]] extends Serializable {

  type Prod[T] = Producer[P, T]

  /**
   * This makes a potentially unsound cast. Since this method is only use
   * in converting from an AlsoProducer to a Literal[T, Prod] below, it is
   * not actually dangerous because we always use it in a safe position.
   */
  protected def mkAlso[T, U]: (Prod[T], Prod[U]) => Prod[U] = { (left, right) =>
    AlsoProducer(left.asInstanceOf[TailProducer[P, T]], right)
  }
  protected def mkAlsoTail[T, U]: (Prod[T], Prod[U]) => Prod[U] = { (left, right) =>
    new AlsoTailProducer(left.asInstanceOf[TailProducer[P, T]],
                         right.asInstanceOf[TailProducer[P, U]])
  }
  protected def mkMerge[T]: (Prod[T], Prod[T]) => Prod[T] = { (left, right) =>
    MergedProducer(left, right)
  }
  protected def mkNamed[T](name: String): (Prod[T] => Prod[T]) = { prod =>
    NamedProducer(prod, name)
  }
  protected def mkTPNamed[T](name: String): (Prod[T] => Prod[T]) = { prod =>
    new TPNamedProducer(prod.asInstanceOf[TailProducer[P, T]], name)
  }
  protected def mkIdentKey[K, V]: (Prod[(K, V)] => Prod[(K, V)]) = { prod =>
    IdentityKeyedProducer(prod)
  }
  protected def mkOptMap[T, U](fn: T => Option[U]): (Prod[T] => Prod[U]) = { prod =>
    OptionMappedProducer(prod, fn)
  }
  protected def mkFlatMapped[T, U](fn: T => TraversableOnce[U]): (Prod[T] => Prod[U]) = { prod =>
    FlatMappedProducer(prod, fn)
  }
  protected def mkKeyFM[T, U, V](fn: T => TraversableOnce[U]): (Prod[(T, V)] => Prod[(U, V)]) = {
    prod =>
      KeyFlatMappedProducer(prod, fn)
  }
  protected def mkValueFM[K, U, V](fn: U => TraversableOnce[V]): (Prod[(K, U)] => Prod[(K, V)]) = {
    prod =>
      ValueFlatMappedProducer(prod, fn)
  }
  protected def mkWritten[T, U >: T](sink: P#Sink[U]): (Prod[T] => Prod[T]) = { prod =>
    WrittenProducer[P, T, U](prod, sink)
  }
  protected def mkSrv[K, T, V](
      serv: P#Service[K, V]): (Prod[(K, T)] => Prod[(K, (T, Option[V]))]) = { prod =>
    LeftJoinedProducer(prod, serv)
  }
  protected def mkSum[K, V](store: P#Store[K, V],
                            sg: Semigroup[V]): (Prod[(K, V)] => Prod[(K, (Option[V], V))]) = {
    prod =>
      Summer(prod, store, sg)
  }

  type LitProd[T] = Literal[Prod, T]

  /**
   * Convert a Producer graph into a Literal in the DagonDag rewriter
   * This is where the tedious work comes in.
   */
  def toLiteral: FunctionK[Prod, LitProd] =
    Memoize.functionK[Prod, LitProd](new Memoize.RecursiveK[Prod, LitProd] {
      import Literal._

      def toFunction[T] = {
        case (s @ Source(_), _) => Const[Prod, T](s)
        case (a: AlsoTailProducer[P, a, T], rec) =>
          Binary[Prod, a, T, T](rec(a.ensure), rec(a.result), mkAlsoTail)
        case (AlsoProducer(ensure, result), rec) =>
          Binary(rec(ensure), rec(result), mkAlso[Any, T])
        case (MergedProducer(l, r), rec) =>
          Binary(rec(l), rec(r), mkMerge)
        case (n: TPNamedProducer[P, T], rec) =>
          Unary(rec(n.producer), mkTPNamed[T](n.id))
        case (NamedProducer(producer, name), rec) =>
          Unary(rec(producer), mkNamed(name))
        case (WrittenProducer(producer, sink), rec) =>
          Unary(rec(producer), mkWritten(sink))
        case (FlatMappedProducer(producer, fn), rec) =>
          Unary(rec(producer), mkFlatMapped(fn))
        case (OptionMappedProducer(producer, fn), rec) =>
          Unary(rec(producer), mkOptMap(fn))
        case (ik: IdentityKeyedProducer[P, k, v], rec) =>
          Unary(rec(ik.producer), mkIdentKey[k, v])
        case (kf: KeyFlatMappedProducer[P, t, u, v], rec) =>
          Unary(rec(kf.producer), mkKeyFM[t, v, u](kf.fn))
        case (vf: ValueFlatMappedProducer[P, k, v, u], rec) =>
          Unary(rec(vf.producer), mkValueFM[k, v, u](vf.fn))
        case (j: LeftJoinedProducer[P, k, v, u], rec) =>
          Unary(rec(j.left), mkSrv[k, v, u](j.joined))
        case (s @ Summer(producer, store, sg), rec) =>
          Unary(rec(producer), mkSum(store, sg))
      }
    })

  /**
   * Create an DagonDag for the given node. This should be the
   * final tail of the graph. You can apply optimizations on this
   * DagonDag and then use the Id returned to evaluate it back to an
   * optimized producer
   */
  def dag[T](p: Producer[P, T]): (DagonDag[Prod], Id[T]) =
    DagonDag[T, Prod](p, toLiteral)

  /**
   * Optimize the given producer according to the rule
   */
  def optimize[T](p: Producer[P, T], rule: Rule[Prod]): Producer[P, T] = {
    val (d, id) = dag(p)
    d(rule).evaluate(id)
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
    def applyWhere[T](on: DagonDag[Prod]) = {
      case NamedProducer(p, _) => p
    }
  }

  /**
   * Identity keyed producer is just a trick to make scala see methods on keyed
   * types, they have no meaning at runtime.
   */
  object RemoveIdentityKeyed extends PartialRule[Prod] {
    def applyWhere[T](on: DagonDag[Prod]) = {
      case IdentityKeyedProducer(p) => p
    }
  }

  /**
   * a.flatMap(fn).flatMap(fn2) can be written as a.flatMap(compose(fn, fn2))
   */
  object FlatMapFusion extends PartialRule[Prod] {
    def applyWhere[T](on: DagonDag[Prod]) = {
      //Can't fuse flatMaps when on fanout
      case FlatMappedProducer(in1 @ FlatMappedProducer(in0, fn0), fn1) =>
        FlatMappedProducer(in0, ComposedFlatMap(fn0, fn1))
    }
  }

  // a.optionMap(b).optionMap(c) == a.optionMap(compose(b, c))
  object OptionMapFusion extends Rule[Prod] {
    def apply[T](on: DagonDag[Prod]) = {
      case OptionMappedProducer(in1 @ OptionMappedProducer(in0, fn0), fn1)
          if (in0.isInstanceOf[Source[_, _]]) =>
        if (on.fanOut(in1) == 1) {
          // only merge options up if we can't merge with a source. Don't destroy the ability to merge
          // with the source
          Some(OptionMappedProducer(in0, ComposedOptionMap(fn0, fn1)))
        } else None
      case OptionMappedProducer(in1 @ OptionMappedProducer(in0, fn0), fn1) =>
        // otherwise always merge since the main cost is serialization + node overhead
        Some(OptionMappedProducer(in0, ComposedOptionMap(fn0, fn1)))
      case _ => None
    }
  }

  /**
   * If you don't care to distinguish between optionMap and flatMap,
   * you can use this rule
   */
  object OptionToFlatMap extends PartialRule[Prod] {
    def applyWhere[T](on: DagonDag[Prod]) = {
      case OptionMappedProducer(in, fn) => in.flatMap(OptionToFlat(fn))
    }
  }

  object OptionThenFlatFusion extends PartialRule[Prod] {
    def applyWhere[T](on: DagonDag[Prod]) = {
      // don't mess with option-map before a source
      case FlatMappedProducer(in1 @ OptionMappedProducer(in0, fn0), fn1)
          if (!in0.isInstanceOf[Source[_, _]]) =>
        in0.flatMap(planner.ComposedOptionFlat(fn0, fn1))
    }
  }

  /**
   * If you can't optimize KeyFlatMaps, use this
   */
  object KeyFlatMapToFlatMap extends PartialRule[Prod] {
    def applyWhere[T](on: DagonDag[Prod]) = {
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
    def applyWhere[T](on: DagonDag[Prod]) = {
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
   *
   * Since we want to minimize the number of nodes, we always perform this optimization.
   */
  object FlatThenOptionFusion extends PartialRule[Prod] {
    def applyWhere[T](on: DagonDag[Prod]) = {
      case OptionMappedProducer(in1 @ FlatMappedProducer(in0, fn0), fn1) =>
        FlatMappedProducer(in0, ComposedFlatMap(fn0, OptionToFlat(fn1)))
    }
  }

  /**
   * (a.flatMap(f1) ++ a.flatMap(f2)) == a.flatMap { i => f1(i) ++ f2(i) }
   */
  object DiamondToFlatMap extends PartialRule[Prod] {
    def applyWhere[T](on: DagonDag[Prod]) = {
      case MergedProducer(left @ FlatMappedProducer(inleft, fnleft),
                          right @ FlatMappedProducer(inright, fnright)) if (inleft == inright) =>
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
    def applyWhere[T](on: DagonDag[Prod]) = {
      case OptionMappedProducer(m @ MergedProducer(a, b), fn) =>
        (a.optionMap(fn)) ++ (b.optionMap(fn))
      case FlatMappedProducer(m @ MergedProducer(a, b), fn) =>
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
    import Literal.Unary

    def apply[T](on: DagonDag[Prod]) = {
      case a @ AlsoProducer(_, _) => None // If we are already an also, we are done
      case MergedProducer(AlsoProducer(tail, l), r) =>
        Some(AlsoProducer(tail, l ++ r))
      case MergedProducer(l, AlsoProducer(tail, r)) =>
        Some(AlsoProducer(tail, l ++ r))
      case node =>
        on.toLiteral(node) match {
          // There are a lot of unary operators, use the literal graph here:
          // note that this cannot be an Also, due to the first case
          case Unary(alsoLit, fn) =>
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

  /**
   * We create a lot of merges followed by maps and merges
   * which summingbird online does not deal with well.
   * Here we optimize those away to reduce the number of nodes
   */
  val standardRule = RemoveIdentityKeyed
    .orElse(MergePullUp)
    .orElse(OptionMapFusion)
    .orElse(FlatMapFusion)
    .orElse(FlatThenOptionFusion)
    .orElse(OptionThenFlatFusion)
    .orElse(DiamondToFlatMap)
}

object DagOptimizer {
  def apply[P <: Platform[P]]: DagOptimizer[P] = new DagOptimizer[P]
}
