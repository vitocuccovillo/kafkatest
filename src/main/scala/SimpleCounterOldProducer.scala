import java.util.Properties
import java.{lang, util}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * RUN WITH ARGS:
 * bootstrap server, topic, [sync|async] delay, numberOfMessages
 * localhost:9092 first sync 500 10
 */
object SimpleCounterOldProducer extends App {

  println(s"Running application with args: ${args.mkString(" ")}")

  var producer: KafkaProducer[String, String] = null
  var props = new Properties()

  val brokerList = args(0)
  val topic = args(1)
  val sync = args(2)
  val delay = args(3).toInt
  val count = args(4).toInt

  this.configure(brokerList, sync)
  this.start()

  val startTime = System.currentTimeMillis()
  println("Starting...")

  this.produce("Starting...")

  (0 to count).foreach(
    { i =>
      produce(i.toString)
      lang.Thread.sleep(delay)
    }
  )

  val endTime = System.currentTimeMillis()
  val terminationMessage = s"Terminate in ${endTime - startTime} ms"
  println(terminationMessage)
  produce(terminationMessage)

  producer.close()

  System.exit(0)


  def configure(brokerList: String, sync: String): Unit = {
    props.put("metadata.broker.list", brokerList)
    props.put("bootstrap.servers", brokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("producer.type", sync)
  }

  def start(): Unit = {
    producer = new KafkaProducer[String, String](props)
  }

  def produce(message: String): Unit = {
    val messageToProduce = new ProducerRecord(topic, "key", message)
    producer.send(messageToProduce)
  }

}
