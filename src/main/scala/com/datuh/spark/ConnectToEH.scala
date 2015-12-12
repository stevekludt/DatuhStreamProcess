/**
  * Created by stevekludt on 12/8/2015.
  */
package main.scala.com.datuh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, _}
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.json4s.jackson.JsonMethods._
import org.json4s._


object ConnectToEH {
  implicit val formats = DefaultFormats

  case class Event(deviceName: String, Temperature: Double, Humidity: Double, EventDateTime: String)

  def main(args : Array[String]): Unit = {
    //EventHub Parameters
    val ehParams = Map[String, String](
      "eventhubs.policyname" -> "Listen",
      "eventhubs.policykey" -> "hFg939EylJQv6M0NgyAz84ttznkvhBOMFHfg9F3QztE=",
      "eventhubs.namespace" -> "datuh-ns",
      "eventhubs.name" -> "sparkeh",
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

    val stringStream: DStream[String] = stream.map(_.toString)

    val sensorDStream: DStream[Array[String]] = stringStream.map(s =>s.split(","))
    val sensorDstream2: DStream[Event] = sensorDStream.map(e=>Event(e(0), e(1).trim.toDouble, e(2).trim.toDouble, e(3)))

    sensorDstream2.foreachRDD(rdd=>rdd.toDF().registerTempTable("MyEvents"))
    //case class Message(msg: String)
    //val sStream = stream.map(msg=>Message(new String(msg))).foreachRDD(rdd=>rdd.toDF().registerTempTable("events"))
    sensorDstream2.print
    ssc.start()
    ssc.awaitTermination()
  }
}