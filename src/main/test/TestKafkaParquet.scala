
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}



object TestKafkaParquet {
  def main(args: Array[String]): Unit = {
    val topic = "test_A"
    //count messages
    val max = 100
    val r = new scala.util.Random(42)



    val kafkaServer = new EmbeddedKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 4)

    val producerThreadTopic = new Thread("Producer thread; controlling termination") {
      override def run() {
        val client = new SimpleKafkaClient(kafkaServer)

        val numbers = 1 to max

        val producer = new KafkaProducer[String, String](client.basicStringStringProducer)
        numbers.foreach { n =>
          producer.send(new ProducerRecord(topic, r.nextInt(3),r.nextString(5),"{\"id\":\""+(n+max)+"\",\"value\":\"value_"+(n+max)+"\"}"))
        }
        Thread.sleep(2000)
        println("*** requesting streaming termination")
      }
    }
    producerThreadTopic.start()
    //Test :
    KafkaParquet.main(Array(kafkaServer.getKafkaConnect,topic))

    kafkaServer.stop()

    println("*** done")
  }
}
