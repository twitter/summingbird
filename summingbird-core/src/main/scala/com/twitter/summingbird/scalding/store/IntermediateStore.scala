package com.twitter.summingbird.scalding.store

import cascading.flow.FlowDef

import com.twitter.bijection.{Injection, Bufferable, Bijection}
import com.twitter.scalding._
import com.twitter.summingbird.batch.{BatchID, Batcher}
import com.twitter.summingbird.scalding.ScaldingEnv

import org.apache.hadoop.io.BytesWritable

import Injection.connect

trait IntermediateStore[K, V] extends Serializable {
  /*
   * Dump the intermediate tuples for a batch. This enables a consumer to
   * recreate all of the monoiod adds in a batch to find the value of a key at a
   * specific time.
   */
  def dump(env: ScaldingEnv, p: TypedPipe[(Long, K, V)], batch: BatchID)
  (implicit fd: FlowDef, mode: Mode): Unit

  /*
   * Given a batch to read, return the dump for the batch.
   * The caller should use the batcher of the IntermediateStore to compute the
   * batch.
   */
  def read(env: ScaldingEnv, batch: BatchID)
    (implicit fd: FlowDef, mode: Mode): TypedPipe[(Long, K, V)]

  def read(env: ScaldingEnv, batches: Iterable[BatchID])
    (implicit fd: FlowDef, mode: Mode): TypedPipe[(Long, K, V)] =
    batches.map { read(env, _) }
      .reduce(_ ++ _)

  /*
   * The batcher to use when computing the BatchID to read.
   */
  def batcher: Batcher
}

/*
 * An IntermedateStore that is backed by SequenceFiles
 */
class IntermediateFileStore[K, V](path: String)
  (implicit kcodec: Injection[K, Array[Byte]], vcodec: Injection[V, Array[Byte]],
    btchr: Batcher)
extends IntermediateStore[K, V] {
  import Dsl._
  import TDsl._

  implicit val kBufferable: Bufferable[K] = Bufferable.viaInjection[K, Array[Byte]]
  implicit val kl2bytes : Injection[(K, Long), Array[Byte]] = Bufferable.injectionOf[(K, Long)]
  implicit val bytes2byteswritable = new BytesWritableBijection

  val kl2writable : Injection[(K, Long), BytesWritable] = connect[(K, Long), Array[Byte], BytesWritable]
  val v2writable : Injection[V, BytesWritable] = connect[V, Array[Byte], BytesWritable]

  override def batcher = btchr

  override def dump(env: ScaldingEnv, p: TypedPipe[(Long, K, V)], batch: BatchID)
  (implicit fd: FlowDef, mode: Mode) {
    // Turn the pipe into a ((k, l), v) pipe and encode it into bytes writable
    val writableBytePipe: TypedPipe[(BytesWritable, BytesWritable)] = p.map {case(l, k ,v) => (kl2writable((k, l)), v2writable(v))}
    val outputPath = getPath(batch)
    val output = WritableSequenceFile[BytesWritable, BytesWritable](outputPath, 'key -> 'val)
    writableBytePipe.toPipe(('key, 'val)).write(output)
  }

  override def read(env: ScaldingEnv, batch: BatchID)
  (implicit fd: FlowDef, mode: Mode): TypedPipe[(Long, K, V)] = {
    val dumpPath = getPath(batch)
    val dumped = WritableSequenceFile[BytesWritable, BytesWritable](dumpPath, 'key -> 'val)
    dumped.read
      .toTypedPipe[(BytesWritable, BytesWritable)]('key -> 'val)
      // Filter out anything that could not be deserialized
      .flatMap {case(kl, v) =>
        for(dkl <- kl2writable.invert(kl);
            dv <- v2writable.invert(v))
        yield (dkl._2, dkl._1, dv)
      }
  }

  private def getPath(batch: BatchID) = path + "/intermediate/%s".format(batch)

}

class BytesWritableBijection extends Bijection[BytesWritable, Array[Byte]] {
  // using take avoids the copy but also avoids returning back padded bytes
  override def apply(a: BytesWritable) = a.getBytes.take(a.getLength)
  override def invert(a: Array[Byte]) = new BytesWritable(a)
}
