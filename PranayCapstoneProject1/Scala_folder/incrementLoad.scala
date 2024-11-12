import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.sql.Timestamp

object incrementLoad extends App {

  // Initialize variables
  val jdbcHostname = "ec2-18-132-73-146.eu-west-2.compute.amazonaws.com"
  val jdbcPort = 5432
  val jdbcDatabase = "testdb"
  val jdbcUsername = "consultants"
  val jdbcPassword = "WelcomeItc@2022"
  val jdbcTable = "amazon_reviews"
  val hdfsOutputPath = "/tmp/amazon_reviews"
  val jdbcUrl = s"jdbc:postgresql://$jdbcHostname:$jdbcPort/$jdbcDatabase"
  val lastLoadedTable = "amazon_reviews_last_loaded"

  case class LastLoadedTimestamp(last_loaded_timestamp: Timestamp)

  // Create a Spark session
  val spark: SparkSession = SparkSession.builder()
    .appName("Load Incremental Table from PostgreSQL to Hadoop")
    .getOrCreate()

  // Step 2: Load the last loaded timestamp
  var last_loaded_timestamp: String = "1970-01-01 00:00:00" // Default to epoch
  try {
    val lastLoadedDf = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", lastLoadedTable)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("driver", "org.postgresql.Driver")
      .load()

    if (!lastLoadedDf.isEmpty) {
      last_loaded_timestamp = lastLoadedDf.collect().head.getString(0)
    }
  } catch {
    case e: Exception =>
      println(s"Error reading last loaded timestamp: ${e.getMessage}")
  }

  // Step 3: Load data from PostgreSQL for incremental load
  val query = s"(SELECT * FROM $jdbcTable WHERE review_timestamp > '$last_loaded_timestamp') AS subquery"
  val df: DataFrame = spark.read
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", query)
    .option("user", jdbcUsername)
    .option("password", jdbcPassword)
    .option("driver", "org.postgresql.Driver")
    .load()

  df.show()

  if (df.count() > 0) {
    df.write
      .mode(SaveMode.Append)
      .option("header","true")
      .csv(hdfsOutputPath)

    // Update the last loaded timestamp
    val maxTimestamp = df.agg("review_timestamp" -> "max").first.getTimestamp(0)
    val updateLastLoadedDf = spark.createDataFrame(Seq(LastLoadedTimestamp(maxTimestamp)))

    updateLastLoadedDf.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", lastLoadedTable)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("driver", "org.postgresql.Driver")
      .mode(SaveMode.Overwrite)
      .save()

    println(s"New data loaded to HDFS, last loaded timestamp updated.")
  } else {
    println("No new data to load.")
  }

  // Stop the Spark session
  spark.stop()
}
