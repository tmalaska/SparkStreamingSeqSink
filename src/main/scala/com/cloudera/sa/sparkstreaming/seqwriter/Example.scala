package com.cloudera.sa.sparkstreaming.seqwriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable

class SeqFileWriterExample {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.out.println("SeqFileWriterExample {host} {port} {outputFolder} {rollInterval} {rollSize} {rollCount} {idleTimeout} {numberOfFilesPerExecutor} {filePrefix} {filePostfix}");
      return ;
    }

    val host = args(0);
    val port = args(1);
    val outputFolder = args(2)
    val rollInterval = args(3).toLong
    val rollSize = args(4).toLong
    val rollCount = args(5).toLong
    val idleTimeout = args(6).toLong
    val numberOfFilesPerExecutor = args(7).toInt
    val filePrefix = args(8)
    val filePostfix = args(9)

    println("host:" + host)
    println("port:" + Integer.parseInt(port))

    val sparkConf = new SparkConf().setAppName("SeqFileWriterExample")
    sparkConf.set("spark.cleaner.ttl", "120000");
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(1))
    val lines = ssc.socketTextStream(host, port.toInt)

    val ss = new StreamingSeq(outputFolder,
      rollInterval,
      rollSize,
      rollCount,
      idleTimeout,
      numberOfFilesPerExecutor,
      filePrefix,
      filePostfix,
      classOf[NullWritable],
      classOf[Text],
      new Configuration,
      sc)

    ss.writeToSeqFiles[String](lines, (t) => (NullWritable.get(), new Text(t)))
    
    ssc.start

  }
}