/**
  * Created by stevekludt on 12/8/2015.
  */
package main.scala.com.datuh.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.EventHubsUtils

object ConnectToEH {
  //implicit val formats = DefaultFormats

  case class Event(deviceName: String,
                   Temperature: Double,
                   Humidity: Double,
                   EventDateTime: String,
                   EventProcessedUtcTime: String,
                   PartitionId: String,
                   EventEnqueuedUtcTime: String)

  def main(args : Array[String]): Unit = {
    //EventHub Parameters
    val ehParams = Map[String, String](
      "eventhubs.policyname" -> "Listen",
      "eventhubs.policykey" -> "C9m+sh8h5jaD4NQqYs1eXKtTDZ7mI9D9Xjqxsr5wML4=",
      "eventhubs.namespace" -> "datuheh-ns",
      "eventhubs.name" -> "datuhdev",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$default",
      "eventhubs.checkpoint.dir" -> "/sparkchelpoint",
      "eventhubs.checkpoint.interval" -> "10"
    )

    val conf = new SparkConf().setAppName("DatuhStream")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val sqlContext = new SQLContext(sc)
    val stream = EventHubsUtils.createUnionStream(ssc, ehParams)
    import sqlContext.implicits._

    val stringStream = stream.map(_.toString)

    val sensorDStream = stringStream.map(s =>s.split(",")).filter(_.length != 0)
    val sensorDstream2 = sensorDStream.map(e=>Event(e(0), e(1).trim.toDouble, e(2).trim.toDouble, e(3), e(4), e(5), e(6)))

    sensorDstream2.foreachRDD(rdd=>rdd.toDF().registerTempTable("MyEvents"))
    //case class Message(msg: String)
    //val sStream = stream.map(msg=>Message(new String(msg))).foreachRDD(rdd=>rdd.toDF().registerTempTable("events"))
    sensorDstream2.print
    ssc.start()
    ssc.awaitTermination()
  }
}