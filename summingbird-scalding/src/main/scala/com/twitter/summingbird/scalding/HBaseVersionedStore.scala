package com.twitter.summingbird.scalding

import cascading.flow.FlowDef
import cascading.tap.Tap
import cascading.tuple.Fields
import com.twitter.algebird.monad.Reader
import com.twitter.bijection.{Injection, AbstractInjection, Bufferable, Codec}
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
    valueInj: Injection[V2, Array[Byte]], override val ordering: Ordering[K]) 
  extends BatchedScaldingStore[K, V] with ReadableStore[K,V2]
{ self =>
  
  val KeyColumnName = "key"
  val ValColumnName = "value"
  val ColumnFamily = "versionedstore"
    
  val scheme = new HBaseScheme(new Fields(KeyColumnName), ColumnFamily, new Fields(ValColumnName))
    
  implicit val b2immutable: Injection[Array[Byte], ImmutableBytesWritable] = 
    new AbstractInjection[Array[Byte], ImmutableBytesWritable] {
      def apply(a: Array[Byte]) = new ImmutableBytesWritable(a)
      override def invert(i: ImmutableBytesWritable) = attempt(i)(_.get())
  }
    
  implicit def kvpInjection: Injection[(K2, V2), (ImmutableBytesWritable,ImmutableBytesWritable)] = {
    Injection.connect[(K2,V2), (Array[Byte],Array[Byte]), (ImmutableBytesWritable,ImmutableBytesWritable)]
  }
  
  // this is only used for client queries and does not need to be serialized out
  // during the scalding job
  @transient val hbaseStore = HBaseByteArrayStore (quorum, table, ColumnFamily, ValColumnName, true)
  hbaseStore.createTableIfRequired
  
  /** 
   *  Exposes a stream with the (K,V) pairs from the highest batchID less than
   *  the input "exclusiveUB" batchID. See readVersions() for the creation of this stream
   *  This method is called by BatchedScaldingStore.merge 
   */
   override def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])] = {
    hbaseStore.createTableIfRequired
    val flow = readVersions(exclusiveUB)
      
    if( flow != Nil ) {
      Right((exclusiveUB, flow))
    } else {
      Left(List("No last batch available < %s for HBaseVersionedStore()".format(exclusiveUB)))
    }
  }
 

  def readVersions(exclusiveUB: BatchID): FlowProducer[TypedPipe[(K, V)]] = Reader { (flowMode: (FlowDef, Mode)) =>
    val mappable = new HBaseVersionedSource[K2, V2](table, scheme)
    
    val filtered = TypedPipe.from(mappable)(flowMode._1, flowMode._2)
                .map{x: (K2, V2) => unpack(x)}
                .filter{ _._1 < exclusiveUB } // (BatchID, (K,V)
                .map{unpacked: (BatchID,(K,V)) => (unpacked._2._1,(unpacked._1,unpacked._2._2))} // (K, (BatchID,V)
    
    object BatchOrdering extends Ordering[(BatchID,V)] {
      def compare (a: (BatchID,V), b: (BatchID,V)) = {
        a._1 compare b._1
      }
    }
    
    implicit def batchOrder = BatchOrdering;
    
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
  
  /* overridden methods for ReadableStore[K, V2] */
  override def get(k: K): Future[Option[V2]] = {
    
    val keyBytes = Injection[K, Array[Byte]](k)
    val valBytes = Await.result(hbaseStore.get(keyBytes))
    
    val v2Val: Option[V2] = valBytes match {
      case Some(bytes) => {
        Injection.invert[V2, Array[Byte]](bytes) match {
          case Success(deserialized) => Option(deserialized)
        }
      }
      // V2 is (BatchID, V)
      case _ => Option((new BatchID(0L), None).asInstanceOf[V2])
    }
    
    Future.value(v2Val)
  }
  
  
  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V2]]] = {
    ks.map{ k => (k, self.get(k)) }
    .toMap
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
