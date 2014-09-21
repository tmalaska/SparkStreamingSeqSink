# SparkStreamingSeqSink
## Overview
Support to write Seq Files with Spark Streaming with similar functionality as Flume HDFS Sink with Seq Files


##Status
This has not been tested yet.  This is just to express an idea.

##Reason
The current Spark Streaming write to Seq files creates a new file per micro batch with is less then optimal.

This functionally will give you the following options 

* rollInterval - how many second before roll
* rollSize - how big before roll
* rollCount - how many records before roll
* idleTimeout - how long before being idea before roll


Also this will let your code look as simple as 

-----

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
-----