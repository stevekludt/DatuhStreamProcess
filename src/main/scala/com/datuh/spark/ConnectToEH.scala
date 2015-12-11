/**
  * Created by stevekludt on 12/8/2015.
  */
package main.scala.com.datuh.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, _}
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.json4s.jackson.JsonMethods._
import org.json4s._


object ConnectToEH {
  implicit val formats = DefaultFormats

  //define the class of the incoming json
  case class Event(EventDateTime: String, Humidity: Double, Temperature: Double, deviceName: String)

  //This is the parser that will parse the Json and apply it to the Event Class
  def parser(json: String): Event = {
    implicit val formats = DefaultFormats

    val parsedJson = parse(json)
    val m = parsedJson.extract[Event]
    m
  }

  def b2s(a: Array[Byte]): String = new String(a)

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
    val streamString =  stream.map(b2s)

    val DeviceName: DStream[Event] = streamString.map(parser)

    val df = DeviceName.foreachRDD(rdd => rdd.toDF().registerTempTable("Events"))

    val currentEvents = sqlContext.sql("Select * from Events")
    currentEvents.collect().foreach(println)

    ssc.start()
    ssc.awaitTermination()
  }
}