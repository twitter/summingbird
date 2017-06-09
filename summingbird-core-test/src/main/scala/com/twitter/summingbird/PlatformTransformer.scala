package com.twitter.summingbird

import scala.collection.mutable

trait PlatformTransformer[P1 <: Platform[P1], P2 <: Platform[P2]] {
  def transformSource[T](source: P1#Source[T]): P2#Source[T]
  def transformSink[T](source: P1#Sink[T]): P2#Sink[T]
  def transformStore[K, V](source: P1#Store[K, V]): P2#Store[K, V]
  def transformService[K, V](source: P1#Service[K, V]): P2#Service[K, V]
}

object PlatformTransformer {
  def apply[P1 <: Platform[P1], P2 <: Platform[P2], T](
    transformer: PlatformTransformer[P1, P2],
    producer: Producer[P1, T]
  ): Producer[P2, T] = Impl(transformer).memoizedProducer(producer)

  private case class Impl[P1 <: Platform[P1], P2 <: Platform[P2]](transformer: PlatformTransformer[P1, P2]) {
    val memoizedMap: mutable.Map[Any, Any] = mutable.Map()

    def memoized[T](source: Any, value: => T): T =
      memoizedMap.getOrElseUpdate(source, value).asInstanceOf[T]

    def memoizedSource[T](source: P1#Source[T]): P2#Source[T] =
      memoized[P2#Source[T]](source, transformer.transformSource(source))
    def memoizedSink[T](sink: P1#Sink[T]): P2#Sink[T] =
      memoized[P2#Sink[T]](sink, transformer.transformSink(sink))
    def memoizedStore[K, V](store: P1#Store[K, V]): P2#Store[K, V] =
      memoized[P2#Store[K, V]](store, transformer.transformStore(store))
    def memoizedService[K, V](service: P1#Service[K, V]): P2#Service[K, V] =
      memoized[P2#Service[K, V]](service, transformer.transformService(service))
    def memoizedProducer[T](producer: Producer[P1, T]): Producer[P2, T] =
      memoized[Producer[P2, T]](producer, transformProducer(producer))

    def transformProducer[T](toTransform: Producer[P1, T]): Producer[P2, T] = toTransform match {
      case Source(source) => Source[P2, T](memoizedSource(source.asInstanceOf[P1#Source[T]]))
      case producer: AlsoTailProducer[P1, _, T] => new AlsoTailProducer[P2, Any, T](
        memoizedProducer[Any](producer.ensure).asInstanceOf[TailProducer[P2, Any]],
        memoizedProducer[T](producer.result).asInstanceOf[TailProducer[P2, T]]
      )
      case AlsoProducer(ensure, result) => AlsoProducer[P2, Any, T](
        memoizedProducer[Any](ensure).asInstanceOf[TailProducer[P2, Any]],
        memoizedProducer[T](result)
      )
      case producer: TPNamedProducer[P1, T] => new TPNamedProducer[P2, T](
        memoizedProducer[T](producer.tail).asInstanceOf[TailProducer[P2, T]],
        producer.id
      )
      case NamedProducer(producer, id) =>
        NamedProducer(memoizedProducer[T](producer), id)
      case OptionMappedProducer(producer, fn) =>
        OptionMappedProducer(memoizedProducer[Any](producer), fn)
      case FlatMappedProducer(producer, fn) =>
        FlatMappedProducer(memoizedProducer[Any](producer), fn)
      case MergedProducer(left, right) =>
        MergedProducer(memoizedProducer[T](left), memoizedProducer[T](right))
      case LeftJoinedProducer(left, joined) => LeftJoinedProducer[P2, Any, Any, Any](
        memoizedProducer[(Any, Any)](left.asInstanceOf[Producer[P1, (Any, Any)]]),
        memoizedService[Any, Any](joined.asInstanceOf[P1#Service[Any, Any]])
      ).asInstanceOf[Producer[P2, T]]
      case Summer(producer, store, semigroup) => Summer[P2, Any, Any](
        memoizedProducer[(Any, Any)](producer.asInstanceOf[Producer[P1, (Any, Any)]]),
        memoizedStore[Any, Any](store.asInstanceOf[P1#Store[Any, Any]]),
        semigroup
      ).asInstanceOf[Producer[P2, T]]
      case WrittenProducer(producer, sink) => WrittenProducer[P2, T, Any](
        memoizedProducer(producer),
        memoizedSink(sink)
      )
      case IdentityKeyedProducer(producer) => IdentityKeyedProducer[P2, Any, Any](
        memoizedProducer[(Any, Any)](producer.asInstanceOf[Producer[P1, (Any, Any)]])
      ).asInstanceOf[Producer[P2, T]]
      case KeyFlatMappedProducer(producer, fn) => KeyFlatMappedProducer[P2, Any, Any, Any](
        memoizedProducer[(Any, Any)](producer.asInstanceOf[Producer[P1, (Any, Any)]]),
        fn.asInstanceOf[(Any) => TraversableOnce[Any]]
      ).asInstanceOf[Producer[P2, T]]
      case ValueFlatMappedProducer(producer, fn) => ValueFlatMappedProducer[P2, Any, Any, Any](
        memoizedProducer[(Any, Any)](producer.asInstanceOf[Producer[P1, (Any, Any)]]),
        fn.asInstanceOf[(Any) => TraversableOnce[Any]]
      ).asInstanceOf[Producer[P2, T]]
    }
  }
}
