
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.json4s.native.JsonMethods.parse
import org.json4s.DefaultFormats

import scala.collection.Iterator

case class Message(
                    id: String,
                    value: String
                  )

object KafkaParquet{
  //private val logger = Logger.getLogger(getClass)
  private implicit val formats = DefaultFormats


  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> args(0),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream_group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topic = args(1)

    val conf = new SparkConf().setAppName("ScalaTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val sql = new SQLContext(sc)

    val rawDataFeed = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
    )

    rawDataFeed.foreachRDD(rdd => {
      println("Partition size: " + rdd.partitions.size)
      println("Partition count: " + rdd.count)
      println("record => " + rdd.toString())
      if (rdd.count() > 0) {
        println("*** " + rdd.getNumPartitions + " partitions")
        rdd.glom().foreach(a => println("*** partition size = " + a.size))
      }
      //repartition 1 for bigger parquet
      val df = sql.createDataFrame(rdd.flatMap(
        record => {
          //println(record.timestamp())
          Iterator.single(parse(record.value().toString).extract[Message])
        }))
      df.show()
      df.repartition(1).write.mode(SaveMode.Append).parquet("test_parquet")
    })
    ssc.start()

    Thread.sleep(4000)
    ssc.stop(stopSparkContext = false, stopGracefully = true)
    try {
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
      }
    }

    // stop Spark
    sc.stop()

  }
}
