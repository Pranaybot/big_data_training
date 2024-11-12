import org.apache.spark.sql.{SparkSession, functions => F, SaveMode}
import org.apache.spark.sql.types._

object batch_processing {
  def main(args: Array[String]): Unit = {

    // Create a Spark session
    val spark: SparkSession = SparkSession.builder()
      .appName("ReviewDataTransformation")
      .getOrCreate()

    // Define the schema manually based on your structure
    val schema = new StructType()
      .add("review_id", IntegerType, nullable = false) // We will auto-increment this
      .add("product_name", StringType, nullable = true)
      .add("user_name", StringType, nullable = false)
      .add("rating", IntegerType, nullable = false)
      .add("review_text", StringType, nullable = true)
      .add("user_city", StringType, nullable = false)
      .add("user_state", StringType, nullable = false)
      .add("product_brand", StringType, nullable = false)
      .add("product_price", DoubleType, nullable = false)
      .add("review_date", DateType, nullable = true)
      .add("review_timestamp", TimestampType, nullable = false)

    // Load the data from HDFS into a DataFrame
    val df = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("/tmp/amazon_reviews")

    // ----------------- 1. Data Cleaning -----------------
    // Trim leading/trailing spaces from text columns
    val dfTrimmed = df.withColumn("product_name", F.trim(F.col("product_name")))
      .withColumn("user_name", F.trim(F.col("user_name")))
      .withColumn("review_text", F.trim(F.col("review_text")))

    // ----------------- 2. Handling Missing or Invalid Values -----------------
    // Filter out rows where essential columns are missing or invalid
    val dfCleaned = dfTrimmed
      .filter(F.col("product_name").isNotNull && F.length(F.col("product_name")) > 0)
      .filter(F.col("user_name").isNotNull && F.length(F.col("user_name")) > 0)
      .filter(F.col("review_text").isNotNull && F.length(F.col("review_text")) > 0)
      .filter(F.col("review_date").isNotNull) // Assuming `review_date` should always be present

    // ----------------- 3. Deduplication -----------------
    // Remove duplicate reviews based on `user_name`, `product_name`, and `review_text`
    val dfDeduplicated = dfCleaned.dropDuplicates("user_name", "product_name", "review_text")

    // ----------------- 4. Ensure `rating` is between 1 and 5 -----------------
    val dfValidRating = dfDeduplicated.filter(F.col("rating").between(1, 5))

    // ----------------- 5. Fill in missing timestamps with current timestamp -----------------
    val dfWithTimestamp = dfValidRating.withColumn("review_timestamp",
      F.when(F.col("review_timestamp").isNull, F.current_timestamp())
        .otherwise(F.col("review_timestamp"))
    )

    // ----------------- Show Final Result -----------------
    dfWithTimestamp.show() // Show the transformed data

    // ----------------- 7. Write the sorted data to HDFS -----------------
    dfWithTimestamp.write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("/tmp/cleaned_amazon_reviews")

    spark.stop()
  }
}
