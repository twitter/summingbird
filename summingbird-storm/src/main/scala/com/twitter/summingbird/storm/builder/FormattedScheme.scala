package com.twitter.summingbird.storm.builder

import com.twitter.tormenta.scheme.Scheme
import java.nio.ByteBuffer
import org.apache.storm.tuple.Fields
import java.util.{ List => JList }
import java.lang.{ Iterable => JIterable }
import scala.collection.JavaConverters.asJavaIterableConverter

/**
 * This is custom wrapping [[Scheme]] class which is a way to customize Scheme's [[OutputFormat]].
 */
private[builder] sealed class FormattedScheme[T](scheme: Scheme[T], format: OutputFormat[T]) extends Scheme[T] {
  override lazy val getOutputFields: Fields = format.asStormFields

  override def deserialize(bytes: ByteBuffer): JIterable[JList[AnyRef]] =
    try {
      encode(decode(bytes))
    } catch {
      case t: Throwable => encode(handle(t))
    }

  private def encode(values: TraversableOnce[T]): JIterable[JList[AnyRef]] =
    values.map(format.injection.apply).toIterable.asJava

  override def decode(buf: ByteBuffer): TraversableOnce[T] = scheme.decode(buf)
}
