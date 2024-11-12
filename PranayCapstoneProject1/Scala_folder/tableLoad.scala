import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.functions.{to_date, to_timestamp}

object tableLoad extends App {

  // Initialize variables inside a method
  val jdbcHostname = "ec2-18-132-73-146.eu-west-2.compute.amazonaws.com"
  val jdbcPort = 5432
  val jdbcDatabase = "testdb"
  val jdbcUsername = "consultants"
  val jdbcPassword = "WelcomeItc@2022"
  val jdbcTable = "amazon_reviews"
  val hdfsOutputPath = "/tmp/amazon_reviews"

  // Construct the JDBC URL
  val jdbcUrl = s"jdbc:postgresql://$jdbcHostname:$jdbcPort/$jdbcDatabase"

  // Create a Spark session
  val spark: SparkSession = SparkSession.builder()
    .appName("Load Full Table from PostgreSQL to Hadoop")
    .getOrCreate()

  // Load data from PostgreSQL
  val df: DataFrame = spark.read
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", jdbcTable)
    .option("user", jdbcUsername)
    .option("password", jdbcPassword)
    .option("driver", "org.postgresql.Driver") // PostgreSQL JDBC driver
    .load()

  // Convert columns to the appropriate data types
  val updatedDf: DataFrame = df
    .withColumn("review_date", to_date(df("review_date"), "yyyy-MM-dd")) // Change the format if needed
    .withColumn("review_timestamp", to_timestamp(df("review_timestamp"))) // Just use to_timestamp without format if already in correct format

  // Show the updated data (optional)
  updatedDf.show()

  // Write the updated DataFrame to HDFS in csv format with header
  updatedDf.write
    .mode(SaveMode.Overwrite) // Overwrite if the output path already exists
    .option("header", "true") // Include the header in the output CSV
    .csv(hdfsOutputPath)

  println(s"Data from table '$jdbcTable' has been successfully loaded to '$hdfsOutputPath' on HDFS.")

  // Stop the Spark session
  spark.stop()
}