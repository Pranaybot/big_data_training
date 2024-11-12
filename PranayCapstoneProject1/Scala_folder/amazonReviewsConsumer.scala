import org.apache.kafka.clients.consumer.KafkaConsumer
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.parser._
import java.sql.{Connection, DriverManager}
import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Using
import scala.util.control.Breaks._
import java.nio.file.{Files, Paths}
import java.io.PrintWriter

object amazonReviewsConsumer {

  case class ReviewMessage(
    product_id: Int,
    user_id: Int,
    date_id: Int,
    rating: Int,
    review_text: String,
    review_timestamp: String
  )

  def main(args: Array[String]): Unit = {
    // Kafka Consumer setup
    val consumerProps = new java.util.Properties()
    consumerProps.put("bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092")
    consumerProps.put("group.id", "consumer-group-1")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("auto.offset.reset", "earliest")

    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(java.util.Collections.singletonList("amazon_reviews_topic"))

    // Consumer for producer timestamps
    val timestampConsumer = new KafkaConsumer[String, String](consumerProps)
    timestampConsumer.subscribe(java.util.Collections.singletonList("amazon_producer_timestamps"))

    // Retrieve producer timestamps
    var producerTimestamps = List[String]()
    for (message <- timestampConsumer.poll(java.time.Duration.ofMillis(100)).asScala) {
      producerTimestamps = message.value().split(",").toList
      break // Use break here to exit after first poll
    }

    // Hive connection setup
    val hiveUrl = "jdbc:hive2://ip-172-31-1-36.eu-west-2.compute.internal:10000/pr_amazon_reviews_db"
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val connection: Connection = DriverManager.getConnection(hiveUrl, "ec2-user", "")

    // Data storage for visuals
    val consumerTimestamps = ListBuffer[String]()
    val timeLags = ListBuffer[Double]()

    // Consumer consumes messages and inserts into Hive
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    breakable {
      for (message <- consumer.poll(java.time.Duration.ofMillis(100)).asScala) {
        val review = decode[ReviewMessage](message.value()).getOrElse(ReviewMessage(0, 0, 0, 0, "", ""))

        // Prepare and execute the insert query
        val insertQuery = s"""
          INSERT INTO fact_reviews_1 (product_id, user_id, date_id, rating, review_text, review_timestamp)
          VALUES (${review.product_id}, ${review.user_id}, ${review.date_id},
                  ${review.rating}, '${review.review_text}', '${review.review_timestamp}')
        """
        Using.resource(connection.createStatement()) { statement =>
          statement.execute(insertQuery)
        }

        // Capture the consumer timestamp
        consumerTimestamps.append(LocalDateTime.now().toString)

        // Calculate the time lag
        if (producerTimestamps.nonEmpty && consumerTimestamps.nonEmpty) {
          val timeLag = Duration.between(
            LocalDateTime.parse(producerTimestamps(consumerTimestamps.length - 1), formatter),
            LocalDateTime.now()
          ).getSeconds.toDouble
          timeLags.append(timeLag)
        }

        // Stop after consuming 50 messages
        if (consumerTimestamps.length >= 50) {
          break // Use break here to exit the loop
        }
      }
    }

    // Close connections
    connection.close()
    consumer.close()
    timestampConsumer.close()

    // Convert the data into a proper format for JSON serialization
    val outputData = Map(
      "consumer_timestamps" -> consumerTimestamps.map(_.toString).toList,
      "time_lags" -> timeLags.map(_.toString).toList // Convert to String for consistent JSON format
    )

    // Save data to a file for visuals
    Using.resource(new PrintWriter(Files.newBufferedWriter(Paths.get("consumer_data.json")))) { writer =>
      writer.write(outputData.asJson.noSpaces)
    }
  }
}
