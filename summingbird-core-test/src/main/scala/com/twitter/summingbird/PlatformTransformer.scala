package com.twitter.summingbird

abstract class PlatformTransformer[P1 <: Platform[P1], P2 <: Platform[P2]] {
  def transformSource[T](source: P1#Source[T]): P2#Source[T]
  def transformSink[T](source: P1#Sink[T]): P2#Sink[T]
  def transformStore[K, V](source: P1#Store[K, V]): P2#Store[K, V]
  def transformService[K, V](source: P1#Service[K, V]): P2#Service[K, V]

  final def transformProducer[T](toTransform: Producer[P1, T]): Producer[P2, T] = toTransform match {
    case Source(source) => Source[P2, T](transformSource(source.asInstanceOf[P1#Source[T]]))
    case producer: AlsoTailProducer[P1, _, T] => new AlsoTailProducer[P2, Any, T](
      transformProducer[Any](producer.ensure).asInstanceOf[TailProducer[P2, Any]],
      transformProducer[T](producer.result).asInstanceOf[TailProducer[P2, T]]
    )
    case AlsoProducer(ensure, result) => AlsoProducer[P2, Any, T](
        transformProducer[Any](ensure).asInstanceOf[TailProducer[P2, Any]],
        transformProducer[T](result)
    )
    case producer: TPNamedProducer[P1, T] => new TPNamedProducer[P2, T](
      transformProducer[T](producer.tail).asInstanceOf[TailProducer[P2, T]],
      producer.id
    )
    case NamedProducer(producer, id) =>
      NamedProducer(transformProducer[T](producer), id)
    case OptionMappedProducer(producer, fn) =>
      OptionMappedProducer(transformProducer[Any](producer), fn)
    case FlatMappedProducer(producer, fn) =>
      FlatMappedProducer(transformProducer[Any](producer), fn)
    case MergedProducer(left, right) =>
      MergedProducer(transformProducer[T](left), transformProducer[T](right))
    case LeftJoinedProducer(left, joined) => LeftJoinedProducer[P2, Any, Any, Any](
      transformProducer[(Any, Any)](left.asInstanceOf[Producer[P1, (Any, Any)]]),
      transformService[Any, Any](joined.asInstanceOf[P1#Service[Any, Any]])
    ).asInstanceOf[Producer[P2, T]]
    case Summer(producer, store, semigroup) => Summer[P2, Any, Any](
      transformProducer[(Any, Any)](producer.asInstanceOf[Producer[P1, (Any, Any)]]),
      transformStore[Any, Any](store.asInstanceOf[P1#Store[Any, Any]]),
      semigroup
    ).asInstanceOf[Producer[P2, T]]
    case WrittenProducer(producer, sink) => WrittenProducer[P2, T, Any](
      transformProducer(producer),
      transformSink(sink)
    )
    case IdentityKeyedProducer(producer) => IdentityKeyedProducer[P2, Any, Any](
      transformProducer[(Any, Any)](producer.asInstanceOf[Producer[P1, (Any, Any)]])
    ).asInstanceOf[Producer[P2, T]]
    case KeyFlatMappedProducer(producer, fn) => KeyFlatMappedProducer[P2, Any, Any, Any](
      transformProducer[(Any, Any)](producer.asInstanceOf[Producer[P1, (Any, Any)]]),
      fn.asInstanceOf[(Any) => TraversableOnce[Any]]
    ).asInstanceOf[Producer[P2, T]]
    case ValueFlatMappedProducer(producer, fn) => ValueFlatMappedProducer[P2, Any, Any, Any](
      transformProducer[(Any, Any)](producer.asInstanceOf[Producer[P1, (Any, Any)]]),
      fn.asInstanceOf[(Any) => TraversableOnce[Any]]
    ).asInstanceOf[Producer[P2, T]]
  }
}
