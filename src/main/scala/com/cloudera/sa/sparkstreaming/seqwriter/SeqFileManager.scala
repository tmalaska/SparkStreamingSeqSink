package com.cloudera.sa.sparkstreaming.seqwriter

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.fs.Path
import java.util.UUID
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.SnappyCodec
import java.util.concurrent.ArrayBlockingQueue
import org.apache.hadoop.fs.FileSystem


object SeqFileManager {
  
  var queue:ArrayBlockingQueue[SeqFileWriterWrapper] = null
  
  def checkoutSeqFile(seqPara: SeqParameters): SeqFileWriterWrapper = {
    this.synchronized {
      if (queue == null) {
        queue = new ArrayBlockingQueue[SeqFileWriterWrapper](seqPara.numberOfFilesPerExecutor)
        val fs = FileSystem.get(seqPara.conf)
        for ( i <- 1 to seqPara.numberOfFilesPerExecutor ) {
          queue.put(new SeqFileWriterWrapper(fs, seqPara))
        }
      }
      queue.take()
    }
    
  }
  
  def returnSeqFile(writer: SeqFileWriterWrapper) {
    
    queue.put(writer)
  }
  
  
}