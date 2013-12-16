package com.twitter.summingbird.scalding

import cascading.flow.FlowDef
import cascading.tap.Tap
import cascading.tuple.Fields
import com.twitter.algebird.monad.Reader
import com.twitter.bijection.hbase.HBaseBijections.ImmutableBytesWritableBijection
import com.twitter.bijection.Injection
import com.twitter.bijection.Inversion.attempt
import com.twitter.maple.hbase.{HBaseScheme, HBaseTap}
import com.twitter.scalding.{AccessMode, Dsl, Mappable, Mode, Source, TupleConverter, TupleSetter, TypedPipe}
import com.twitter.scalding.typed.TypedSink
import com.twitter.storehaus.hbase.HBaseByteArrayStore
import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.batch.{Batcher, BatchID}
import com.twitter.util.{Await, Future}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import scala.util.{Failure, Success}

import Injection._

/**
 * Scalding implementation of the batch read and write components of a
 * store that uses the VersionedKeyValSource from scalding-commons.
 *
 * @author Josh Buffum
 */


object HBaseVersionedStore {   
  
  def apply[K, V](quorum: Seq[String],
                          table: String)(
    implicit
      batcher: Batcher,
      injection: Injection[(K, (BatchID,V)), (Array[Byte], Array[Byte])],
      keyInj: Injection[K, Array[Byte]],
      valueInj: Injection[(BatchID,V), Array[Byte]],
      ordering: Ordering[K]): HBaseVersionedStore[K, V, K, (BatchID,V)] = {
    new HBaseVersionedStore[K, V, K, (BatchID,V)](quorum, table, batcher)(
        { case (batchID, (k, v)) => (k, (batchID.next, v)) })(
        { case (k, (batchID, v)) => (batchID, (k, v)) })
  }
}

class HBaseVersionedStore [K, V, K2, V2](quorum: Seq[String],
                                         table: String,
                                         override val batcher: Batcher)
  (pack: (BatchID, (K, V)) => (K2, V2))
  (unpack: ((K2, V2)) => (BatchID, (K,V)))(
  implicit     
    injection: Injection[(K2, V2), (Array[Byte], Array[Byte])],
    keyInj: Injection[K, Array[Byte]],
    valueInj: Injection[V2, Array[Byte]], override val ordering: Ordering[K]) extends BatchedScaldingStore[K, V]
{
  val KeyColumnName = "key"
  val ValColumnName = "value"
  val ColumnFamily = "versionedstore"
    
  val scheme = new HBaseScheme(new Fields(KeyColumnName), ColumnFamily, new Fields(ValColumnName))
   
  implicit lazy val byteArray2BytesWritableInj : Injection[Array[Byte], ImmutableBytesWritable] = fromBijection[Array[Byte], ImmutableBytesWritable](ImmutableBytesWritableBijection[Array[Byte]])
  
  implicit def kvpInjection: Injection[(K2, V2), (ImmutableBytesWritable,ImmutableBytesWritable)] = {
    Injection.connect[(K2,V2), (Array[Byte],Array[Byte]), (ImmutableBytesWritable,ImmutableBytesWritable)]
  }

  
  // this is only used for client queries and does not need to be serialized out
  // during the scalding job
  @transient val hbaseStore =  HBaseByteArrayStore (quorum, table, ColumnFamily, ValColumnName, true)
    .convert[K,V2](keyInj)(valueInj)
  
  /** 
   *  Exposes a stream with the (K,V) pairs from the highest batchID less than
   *  the input "exclusiveUB" batchID. See readVersions() for the creation of this stream
   *  This method is called by BatchedScaldingStore.merge 
   */
   override def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])] = {
    Right((exclusiveUB, readVersions(exclusiveUB)))
  }
 

  def readVersions(exclusiveUB: BatchID): FlowProducer[TypedPipe[(K, V)]] = Reader { (flowMode: (FlowDef, Mode)) =>
    val mappable = new HBaseVersionedSource[K2, V2](table, scheme)
    
    val filtered = TypedPipe.from(mappable)(flowMode._1, flowMode._2)
                .map{x: (K2, V2) => unpack(x)}
                .filter{ _._1 < exclusiveUB } // (BatchID, (K,V)
                .map{unpacked: (BatchID,(K,V)) => (unpacked._2._1,(unpacked._1,unpacked._2._2))} // (K, (BatchID,V)
    
    implicit def batchOrderer = Ordering.by[(BatchID,V),BatchID](_._1)
    
    filtered
      .group
      .max
      .map{x: (K, (BatchID,V)) => (x._1, x._2._2)}
  }
    
  
  /**
    * write the (K, V) pairs aggregated up to batchID (inclusive) into the 
    * BatchedScaldingStore. In our case, this BatchedScaldingStore uses HBase
    * as the mechanism to actually store data
    * 
    * The data is written in serialized pairs of (K, (BatchID, V)) 
    */
  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit = {
    import Dsl._
    
    lastVals.map{x: (K,V) => Injection[(K2,V2),(ImmutableBytesWritable,ImmutableBytesWritable)](pack(batchID, x))}
      .toPipe(new Fields(KeyColumnName,ValColumnName))
      .write(new HBaseVersionedSource[K2, V2](table, scheme))
  }  
  
  
  def toReadableStore: ReadableStore[K,V2] = {
    hbaseStore
  }  

}


class HBaseVersionedSource[K, V](table: String,
                                 scheme: HBaseScheme )(
  implicit injection: Injection[(K, V), (Array[Byte], Array[Byte])])
  extends Source with Mappable[(K,V)] with TypedSink[(K,V)] 
{
  override def converter[U >: (K, V)] = TupleConverter.asSuperConverter[(K, V), U](TupleConverter.of[(K, V)])

  override def setter[U <: (K, V)] = TupleSetter.asSubSetter[(K, V), U](TupleSetter.of[(K,V)])
  
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_,_,_] = {
    (new HBaseTap(table, scheme)).asInstanceOf[Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]]]
  }
}
