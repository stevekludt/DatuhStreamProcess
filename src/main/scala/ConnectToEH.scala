/**
  * Created by stevekludt on 12/8/2015.
  */
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

object DatuhStream {
  import org.json4s._
  implicit val formats = DefaultFormats

  //define the class of the incoming json
  case class Event(EventDateTime: String, Humidity: Double, Temperature: Double, deviceName: String)

  //This is the parser that will parse the Json and apply it to the Event Class
  def parser(json: String): String = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = DefaultFormats

    val parsedJson = parse(json)
    val m = parsedJson.extract[Event]
     m.deviceName
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
    val stream = EventHubsUtils.createUnionStream(ssc, ehParams)
    val sqlContext = new SQLContext(sc)

    val streamString =  stream.map(b2s)

    val DeviceName = streamString.map(parser)

    DeviceName.print()

    ssc.start()
  }
  /**

  case class Message(msg: String)

    val streamstring = stream.map(msg => Message(new String(msg)))
  case class Message(msg: String)
  stream.map(msg => Message(new String(msg))).foreachRDD(rdd => rdd.toDF().registerTempTable("events"))
  /** This creates a new DStream of "data", then writes that file out to hdfs
    * the problem with this is that it create a file in hdfs for each dstream
    */
  val windowedData = stream.map(record => record).window(Seconds(60), Seconds(60))
  /** this works, but is putting the test "Message" (the name of the class) in for each records */
  stream.map(msg => Message(new String(msg))).saveAsTextFiles("wasb://tempsim@datuhhdfs.blob.core.windows.net/spark/incoming/events")

  /** NEXT TODO - need to map the stream into a class and parse out the json
    * http://json4s.org/
    * */

  val parsedJson = {
    stream.foreachRDD(rdd => rdd.map(msg => parse(msg).extract[Event]))
  }


  /** Here we are going to take a hard Coded JSON String and use it to create schema
for a Spark SQL Table */
  val jsonString = sc.parallelize( """{"deviceName":"A234","Temperature":337.8,"Humidity":0.3624,"EventDateTime":"2015-11-23 15:40:56"}""" :: Nil)

  val schema = sqlContext.jsonRDD(jsonString)

  schema.registerTempTable("StreamTable")

  /** Now we're going to create a hive ORC stream table so that we can read the data from
exteral systems via Hive  */
  /**
  import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._

val hiveContext = new org.apache.spark.sql.hive.hiveContext(sc)

hiveContext.sql("CREATE TABLE EventData_orc (DeviceName String,	Temperature FLOAT,	Humidity FLOAT,
	EventDateTime TIMESTAMP) stored as orc")
    */
  /** start the stream */
  stream.print
  ssc.start()

  /**
    * need to add this when submitting in a program
    * ssc.awaitTermination()
    */
    */
}