package com.cloudera.sa.sparkstreaming.seqwriter

import org.apache.hadoop.io.Writable
import scala.reflect.ClassTag
import org.apache.hadoop.conf.Configuration

class SeqParameters (val outputFolder: String, 
      val rollInterval: Long, 
      val rollSize: Long, 
      val rollCount: Long, 
      val idleTimeout: Long,
      val numberOfFilesPerExecutor: Integer,
      val filePrefix: String,
      val filePostfix: String,
      val keyType: Class[_],
      val valueType: Class[_],
      val conf: Configuration){

}