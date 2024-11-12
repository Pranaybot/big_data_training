import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import akka.actor.ActorSystem
import scala.util.Random
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, Instant}
import java.nio.file.{Files, Paths}
import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
import scala.util.Using
import io.circe.generic.auto._
import io.circe.syntax._

object amazonReviewsProducer {


  // Define case class for review messages
  case class ReviewMessage(
    product_id: Int,
    user_id: Int,
    date_id: Int,
    rating: Int,
    review_text: String,
    review_timestamp: String
  )

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("kafka-producer-system")

    // Kafka Producer setup
    val brokers = List(
      "ip-172-31-13-101.eu-west-2.compute.internal:9092",
      "ip-172-31-3-80.eu-west-2.compute.internal:9092",
      "ip-172-31-5-217.eu-west-2.compute.internal:9092",
      "ip-172-31-9-237.eu-west-2.compute.internal:9092"
    )

    val producerProps = new java.util.Properties()
    producerProps.put("bootstrap.servers", brokers.mkString(","))
    producerProps.put("key.serializer", classOf[StringSerializer].getName)
    producerProps.put("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](producerProps)

    // Data storage for visuals
    val producerTimestamps = ListBuffer[String]()
    val ratings = ListBuffer[Int]()
    val messageSizes = ListBuffer[Int]()

    // Prepare date formatter
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    // Producer sends messages (Simulated review data)
    for (i <- 0 until 50) {
      val message = ReviewMessage(
        product_id = Random.nextInt(1000) + 1,
        user_id = Random.nextInt(500) + 1,
        date_id = LocalDateTime.now().format(formatter).toInt,
        rating = Random.nextInt(5) + 1,
        review_text = s"Sample review text $i",
        review_timestamp = LocalDateTime.now().toString
      )

      // Convert the case class to JSON using circe
      val messageJson = message.asJson.noSpaces

      val producerRecord = new ProducerRecord[String, String]("amazon_reviews_topic", messageJson)
      producer.send(producerRecord)

      producerTimestamps.append(Instant.now().toString)
      ratings.append(message.rating)
      messageSizes.append(message.review_text.length)

      // Simulate a delay
      Thread.sleep((100 + Random.nextInt(400)).toLong)
    }

    // Send producer timestamps for use in consumer
    producer.send(new ProducerRecord[String, String]("amazon_producer_timestamps", producerTimestamps.mkString(",")))
    producer.close()

    // Convert the Map[String, List[Any]] to Map[String, List[String]] for serialization
    val outputData: Map[String, List[String]] = Map(
      "producer_timestamps" -> producerTimestamps.toList.map(_.toString),
      "ratings" -> ratings.toList.map(_.toString)
      ,
      "message_sizes" -> messageSizes.toList.map(_.toString)
    )

    // Write the output data to a JSON file using circe
    Using.resource(new PrintWriter(Files.newBufferedWriter(Paths.get("producer_data.json")))) { writer =>
      writer.write(outputData.asJson.noSpaces)
    }

    // Terminate the actor system when done
    system.terminate()
  }
}
