package com.twitter.summingbird.scalding

import cascading.flow.FlowDef
import cascading.tap.Tap
import cascading.tuple.Fields
import com.twitter.algebird.monad.Reader
import com.twitter.bijection.hbase.HBaseBijections.ImmutableBytesWritableBijection
import com.twitter.bijection.{Bufferable, Injection}
import com.twitter.bijection.Inversion.attempt
import com.twitter.maple.hbase.{HBaseScheme, HBaseTap}
import com.twitter.scalding.{AccessMode, Dsl, Mappable, Mode, Source, TupleConverter, TupleSetter, TypedPipe}
import com.twitter.scalding.typed.TypedSink
import com.twitter.storehaus.hbase.HBaseByteArrayStore
import com.twitter.storehaus.ReadableStore
import com.twitter.summingbird.batch.{Batcher, BatchID}
import com.twitter.summingbird.batch.BatchID.batchID2Bytes
import com.twitter.util.{Await, Future}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper, ZooDefs}
import org.apache.zookeeper.data.Stat
import scala.util.{Failure, Success, Try}

import Injection._

/**
 * Scalding implementation of the batch read and write components of a
 * store that uses the HBase Tap from maple and HBase store from Storehaus
 *
 * @author Josh Buffum
 */


object HBaseVersionedStore {   
    
    /**
    * Returns a HBaseVersionedStore abstracts the store/retrieval
    * of (K,V) pairs. These (K,V) pairs are associated with a 
    * BatchID by internally storing (K, (BatchID,V)). 
    *
    * The packing function receives the inclusive upper BatchID being
    * committed. We actually need to store the exclusive upper bound
    * alongside the value, so the packing function calls
    * batchID.next. On the unpack, we drop the batchID, so no
    * off-by-one error arises.
    */
  def apply[K, V](quorum: Seq[String],
                  table: String)(
    implicit
      batcher: Batcher,
      keyInj: Injection[K, Array[Byte]],
      valueInj: Injection[V, Array[Byte]],
      ordering: Ordering[K]): HBaseVersionedStore[K, V, K, (BatchID,V)] = {
    
    implicit val buf = Bufferable.viaInjection[(BatchID, V), (Array[Byte], Array[Byte])]
    def value2Inj : Injection[(BatchID, V), Array[Byte]] = Bufferable.injectionOf[(BatchID,V)]
    
    new HBaseVersionedStore[K, V, K, (BatchID,V)](quorum, table, batcher)(
        { case (batchID, (k, v)) => (k, (batchID.next, v)) })(
        { case (k, (batchID, v)) => (batchID, (k, v)) })(keyInj, keyInj, value2Inj, value2Inj, ordering)
  }
}

class HBaseVersionedStore [K, V, K2, V2](quorum: Seq[String],
                                         table: String,
                                         override val batcher: Batcher)
  (pack: (BatchID, (K, V)) => (K2, V2))
  (unpack: ((K2, V2)) => (BatchID, (K,V)))(
  implicit     
    keyInj: Injection[K, Array[Byte]],
    key2Inj: Injection[K2, Array[Byte]],
    valueInj: Injection[(BatchID,V), Array[Byte]],
    value2Inj: Injection[V2, Array[Byte]], override val ordering: Ordering[K]) extends BatchedScaldingStore[K, V]
{
  val KeyColumnName = "key"
  val ValColumnName = "value"
  val ColumnFamily = "versionedstore"
    
  val scheme = new HBaseScheme(new Fields(KeyColumnName), ColumnFamily, new Fields(ValColumnName))
   
  implicit def byteArray2BytesWritableInj : Injection[Array[Byte], ImmutableBytesWritable] = fromBijection[Array[Byte], ImmutableBytesWritable](ImmutableBytesWritableBijection[Array[Byte]])
  
  implicit def injection : Injection[(K2, V2), (Array[Byte], Array[Byte])] = tuple2[K2, V2, Array[Byte], Array[Byte]](key2Inj, value2Inj)
  
  implicit def kvpInjection: Injection[(K2, V2), (ImmutableBytesWritable,ImmutableBytesWritable)] = {
    Injection.connect[(K2,V2), (Array[Byte],Array[Byte]), (ImmutableBytesWritable,ImmutableBytesWritable)]
  }
  
  // storehaus store
  def hbaseStore = HBaseByteArrayStore (quorum, table, ColumnFamily, ValColumnName, true)
    .convert[K, (BatchID,V)](keyInj)(valueInj)
    
  // state store for last processed batchID (readLast)
  def zk = new HBaseStoreZKState(quorum, table)
  
  /** 
   *  Exposes a stream with the (K,V) pairs from the highest batchID less than
   *  the input "exclusiveUB" batchID. See readVersions() for the creation of this stream
   *  This method is called by BatchedScaldingStore.merge 
   */
  override def readLast(exclusiveUB: BatchID, mode: Mode): Try[(BatchID, FlowProducer[TypedPipe[(K, V)]])] = {
    readLastBatchID match {
      case Some(batchID) if batchID < exclusiveUB => Right((exclusiveUB, readVersions(exclusiveUB)))    
      case Some(batchID) => Left(List("No last batch available < %s for HBaseVersionedStore".format(exclusiveUB)))
      case None => Left(List("No last batch available for HBaseVersionedStore"))
    }
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
    * 
    * In addition to writing the (K, (BatchID, V)) tuples, we also store away
    * the last BatchID processed into ZooKeeper to be later used by readLastBatchID
    * for flow planning. We do this as close to the time of writing to HBase as possible
    * to try to keep ZooKeeper and HBase in sync.
    * TODO: In the event that https://github.com/twitter/summingbird/issues/214 
    * is resolved, it might be nice to see if we can include this task in a registered 
    * Watcher on the WaitingState
    */
  override def writeLast(batchID: BatchID, lastVals: TypedPipe[(K, V)])(implicit flowDef: FlowDef, mode: Mode): Unit = {
    import Dsl._
    
    var wroteLastBatch = false;
    
    lastVals
      .map{x: (K,V) => {
        if( !wroteLastBatch ) {
          zk.setLastBatchID(batchID)
          wroteLastBatch = true
        }
        Injection[(K2,V2),(ImmutableBytesWritable,ImmutableBytesWritable)](pack(batchID, x))}
      }
      .toPipe(new Fields(KeyColumnName,ValColumnName))
      .write(new HBaseVersionedSource[K2, V2](table, scheme))
  }  
  
  
  def toReadableStore: ReadableStore[K,(BatchID,V)] = {
    hbaseStore
  }  
  
  def readLastBatchID : Option[BatchID] = {
    zk.getLastBatchID
  }
  
  class HBaseStoreZKState (quorum: Seq[String],
                           table: String) 
    extends Watcher 
  {
    val LastBatchIDZKPath = "/summingbird/" + table + "/lastBatchID"
    val zkServers = quorum.mkString(",")
    val DefaultZKSessionTimeout = 4000
    
    val zk = new ZooKeeper(zkServers, DefaultZKSessionTimeout, this)
    createZKPath(LastBatchIDZKPath.split("/"), 2)
    
    def createZKPath(subpaths: Array[String], idx: Int) {
        val subpath = (subpaths.slice(0,idx).mkString("/"))
        
        if( zk.exists(subpath, false) == null ) {
          zk.create(subpath, batchID2Bytes(BatchID.Min), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        }  
        
        if( subpaths.size != (idx-2)) {
          createZKPath(subpaths, idx+1)
        }
    }
    
    def setLastBatchID(batchID: BatchID) : scala.util.Try[Stat] = {
      scala.util.Try {
        zk.setData(LastBatchIDZKPath, batchID2Bytes(batchID), -1)
      }
    }
    
    def getLastBatchID() : Option[BatchID] = {
      val batchBytes = zk.getData(LastBatchIDZKPath, false, new Stat())
      
      batchID2Bytes.invert(batchBytes) match {
        case Success(batchID) => Some(batchID)
        case Failure(ex) => None
      }
    }
    
    override def process(event: WatchedEvent) = {    
    }
  }
      
  private class HBaseVersionedSource[K, V](table: String,
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
}
