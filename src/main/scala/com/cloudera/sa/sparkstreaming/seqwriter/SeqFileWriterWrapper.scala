package com.cloudera.sa.sparkstreaming.seqwriter

import java.util.UUID
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.fs.FileSystem
import java.util.Timer
import java.util.TimerTask

class SeqFileWriterWrapper(fs: FileSystem, seqPara: SeqParameters) {

  var filePath = seqPara.outputFolder + "/" + seqPara.filePrefix + "." + UUID.randomUUID + ".tmp"
  var writer: SequenceFile.Writer = null
  var linesWriten: Long = 0
  var timer: Timer = new Timer

  var timeOpenedFile: Long = System.currentTimeMillis()
  var lastTouched: Long = System.currentTimeMillis()

  def createNewFileWriter {
    this.synchronized {
      val optPath = SequenceFile.Writer.file(new Path(filePath))
  
      val optKey = SequenceFile.Writer.keyClass(seqPara.keyType)
  
      val optVal = SequenceFile.Writer.valueClass(seqPara.valueType)
  
      val optCom = SequenceFile.Writer.compression(CompressionType.RECORD, new SnappyCodec)
  
      writer = SequenceFile.createWriter(seqPara.conf, optPath, optKey, optVal, optCom);
  
      linesWriten = 0
      timeOpenedFile = System.currentTimeMillis()
      lastTouched = System.currentTimeMillis()
  
      if (seqPara.idleTimeout > 0 && seqPara.rollInterval > 0) {
        timer.schedule(new timeChecker(seqPara), 1000)
      }
    }
  }

  private class timeChecker(seqPara: SeqParameters) extends TimerTask {
    @Override def run {
      this.synchronized {
        if (writer != null) {
          if ((seqPara.rollInterval > 0 && System.currentTimeMillis() - timeOpenedFile > seqPara.rollInterval) ||
            (seqPara.idleTimeout > 0 && System.currentTimeMillis() - lastTouched > seqPara.idleTimeout)) {
            close
          } else {
            timer.schedule(new timeChecker(seqPara), 1000)
          }
        }
      }
    }
  }

  def append(k: Writable, v: Writable) {
    this.synchronized {
      if (writer == null) {
        createNewFileWriter
      }
      writer.append(k, v)

      linesWriten += 1
      lastTouched = System.currentTimeMillis()

      if ((seqPara.rollCount > 0 && linesWriten >= seqPara.rollCount ) ||
          (seqPara.rollSize > 0 && seqPara.rollSize > writer.getLength())) {
        close
      }
    }
  }

  def sync {
    this.synchronized {
      writer.sync()
    }
  }
  
  def close {
    this.synchronized {
      writer.close()
      val finalFileName = seqPara.outputFolder + "/" + seqPara.filePrefix + "." + UUID.randomUUID + seqPara.filePostfix
      fs.rename(new Path(filePath), new Path(finalFileName))
      writer = null
    }
  }
}