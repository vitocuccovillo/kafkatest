import java.util
import java.util.{Collections, Properties}

import kafka.serializer.StringDecoder
import kafka.utils.VerifiableProperties
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}

/**
 * HighLevel consumer per una media dinamica
 * dei valori ricevuti
 *
 * zookeeper: coordinare tutti i consumer in un consumer group
 * e tener traccia degli offset
 *
 * runnare con questi valori:
 * istanza di zookeeper
 * groupId: per consentire di bilanciare il carico di lavoro
 * topic
 * una window di 10 messaggi
 * un timeout di 120.000 ms, 2 minuti
 * localhost:9092 avg first 10 120000
 */
object SimpleCounterOldConsumer extends App{

  var consumer: KafkaConsumer[String, String] = null
  var next: String = null
  var num: Int = 0

  val zkUrl = args(0)
  val groupId = args(1)
  val topic = args(2)
  val window = args(3).toInt
  val waitTime = args(4)

  val stream = new util.HashMap[String, String]
  val buffer = new CircularFifoQueue[Int](window)

  val config = this.getConsumerConfiguration(zkUrl, groupId)
  this.start(topic, config)
  var prevBufferSize = 0

  while ((next = this.getNextMessage()) != null) {
    var sum: Int = 0
    try {
      num = next.toInt
      buffer.add(num)
    } catch {
      case e: NumberFormatException => {

      }
    }

    //iterate the buffer
    val bufferIterator = buffer.iterator()
    while (bufferIterator.hasNext) sum += bufferIterator.next()

    if (buffer.size > prevBufferSize) {
      prevBufferSize += 1
      println(s"Buffer size is: ${buffer.size} and moving avg is ${sum / buffer.size}")
    }

    //decommenta per committare gli offsets ad ogni messaggio
    // movingAvg.comnsumer.commitOffsets()
  }

  consumer.close()
  System.exit(0)

  private def start(topic: String, props: Properties): Unit = {

    println(s"Starting consumer on topic: $topic")

    //indicare a kafka quanti thread leggeranno ogni topic, ora solo 1
    consumer = new KafkaConsumer[String, String](props)

    val topicCountMap = new util.HashMap[String, Int]()
    // 1 topic, 1 thread
    topicCountMap.put(topic, new Integer(1))

    /**
     * serve un decoder per convertire i messaggi a stringa
     */
    val stringDecoder = new StringDecoder(new VerifiableProperties())

    /**
     * kafka fornisce una lista di stream di messaggi per ogni topic,
     * prendiamo solo quelli del topic che ci interessa
     * prendiamo il primo thread (ossia 0)
     */
    consumer.subscribe(Collections.singletonList(topic))
  }

  private def getNextMessage(): String = {
    val consumerIterator = consumer.poll(1000).iterator()
    try {
      if (consumerIterator.hasNext) consumerIterator
          .next()
          .value()
      else null
    } catch {
      case e: TimeoutException => {
        println(s"Waited more than $waitTime too much!!!")
        null
      }
    }
  }

  private def getConsumerConfiguration(zkUrl: String, groupId: String): Properties = {
    val props = new  Properties()
    props.put("zookeeper.connect", zkUrl)
    props.put("bootstrap.servers", zkUrl)
    props.put("group.id", groupId)
    props.put("auto.commit.interval.ms", "1000") // quanto spesso si muove lì'offset
    props.put("auto.offset.reset", "latest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    /**
     * decommentare se si vuole fare il commit manuale dei messaggi
     */
    //props.put("auto.commit.enable","false")

    /**
     * commentare se non si vogliono attendere dati all'infinito
     */
    props.put("consumer.timeout.ms", waitTime)
    props

  }

}
