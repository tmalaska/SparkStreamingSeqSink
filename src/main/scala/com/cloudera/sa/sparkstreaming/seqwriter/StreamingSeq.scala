package com.cloudera.sa.sparkstreaming.seqwriter

import org.apache.spark.SparkContext
import org.apache.hadoop.io.Writable
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable

class StreamingSeq(@transient outputFolder: String, 
  @transient rollInterval: Long, 
  @transient rollSize: Long, 
  @transient rollCount: Long, 
  @transient idleTimeout: Long, 
  @transient numberOfFilesPerExecutor: Integer, 
  @transient filePrefix: String, 
  @transient filePostfix: String, 
  @transient keyType: Class[_], 
  @transient valueType: Class[_], 
  @transient conf: Configuration, 
  @transient sc: SparkContext) {

  val seqParameters = sc.broadcast(new SeqParameters(outputFolder,
    rollInterval, rollSize, rollCount, idleTimeout, numberOfFilesPerExecutor,
    filePrefix, filePostfix, keyType, valueType, conf))

  def writeToSeqFiles[T](dstream: DStream[T],
    f: (T) => (Writable, Writable)) = {
    dstream.foreach((rdd, time) => {

      val seqWriter = SeqFileManager.checkoutSeqFile(seqParameters.value)

      rdd.foreachPartition(it =>
        it.foreach(t => {
          val keyValue = f(t)
          seqWriter.append(keyValue._1, keyValue._2)
        }))
        
      seqWriter.sync
      SeqFileManager.returnSeqFile(seqWriter)
    })
  }

}