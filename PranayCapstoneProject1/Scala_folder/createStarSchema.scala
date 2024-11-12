// Import necessary Spark libraries
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object createStarSchema extends App {

  // Initialize Spark session
  val spark = SparkSession.builder()
    .appName("AmazonReviewsDimensionalModeling")
    .getOrCreate()

  // Read the CSV files into a DataFrame from the cleaned_amazon_reviews folder
  val reviewsDF = spark.read
    .option("header", "true")  // Assuming the CSV files have headers
    .option("inferSchema", "true")  // Automatically infers data types for each column
    .csv("/tmp/cleaned_amazon_reviews/*.csv")  // Path to the CSV files

  // Define a window specification for ordering by review_id
  val windowSpec = Window.orderBy("review_id")

  // Add product_id, user_id, and date_id columns according to the order of review_id
  val reviewsWithIdsDF = reviewsDF
    .withColumn("product_id", row_number().over(windowSpec) - 1)
    .withColumn("user_id", row_number().over(windowSpec) - 1)
    .withColumn("date_id", row_number().over(windowSpec) - 1)

  val factTableSchema = new StructType()
    .add("product_id", IntegerType, nullable = false)
    .add("user_id", IntegerType, nullable = false)
    .add("date_id", IntegerType, nullable = false)
    .add("rating", IntegerType, nullable = false)
    .add("review_text", StringType, nullable = true)
    .add("review_timestamp", TimestampType, nullable = false)

  val productTableSchema = new StructType()
    .add("product_id", IntegerType, nullable = false)
    .add("product_name", StringType, nullable = true)
    .add("product_brand", StringType, nullable = false)
    .add("product_price", DoubleType, nullable = false)

  val userTableSchema = new StructType()
    .add("user_id", IntegerType, nullable = false)
    .add("user_name", StringType, nullable = false)
    .add("user_city", StringType, nullable = false)
    .add("user_state", StringType, nullable = false)

  val dateTableSchema = new StructType()
    .add("date_id", IntegerType, nullable = false)
    .add("review_date", DateType, nullable = true)
    .add("year", IntegerType, nullable = true)
    .add("month", IntegerType, nullable = true)
    .add("day", IntegerType, nullable = true)

  // Select only the columns needed for the fact table (removing text columns)
  val factTableDF = reviewsWithIdsDF
    .select("product_id", "user_id", "date_id",
    "rating", "review_text", "review_timestamp")

  // Assuming factTableDF is already created as shown in your code
  val factTableDFWithSchema: DataFrame = spark.createDataFrame(
    factTableDF.rdd, // Convert the DataFrame to RDD
    factTableSchema // Apply the schema
  )

  // Show the fact table data (optional)
  factTableDFWithSchema.show(5)

  // Create dimensional tables
  // Create product dimension table
  val productDimDF = reviewsWithIdsDF
    .select("product_id", "product_name",
      "product_brand", "product_price")
    .distinct()

  val productDimDFWithSchema: DataFrame = spark.createDataFrame(
    productDimDF.rdd, // Convert the DataFrame to RDD
    productTableSchema // Apply the schema
  )

  // Create user dimension table
  val userDimDF = reviewsWithIdsDF
    .select("user_id", "user_name",
      "user_city", "user_state")
    .distinct()

  val userDimDFWithSchema: DataFrame = spark.createDataFrame(
    userDimDF.rdd, // Convert the DataFrame to RDD
    userTableSchema // Apply the schema
  )

  // Create date dimension table with year, month, and day
  val dateDimDF = reviewsWithIdsDF
    .withColumn("year", year(col("review_date")))
    .withColumn("month", month(col("review_date")))
    .withColumn("day", dayofmonth(col("review_date")))
    .select("date_id", "review_date", "year", "month", "day")  // Now selecting all columns
    .distinct()

  val dateDimDFWithSchema: DataFrame = spark.createDataFrame(
    dateDimDF.rdd, // Convert the DataFrame to RDD
    dateTableSchema // Apply the schema
  )

  // Show dimension tables
  productDimDF.show()
  userDimDF.show()
  dateDimDF.show()

  // Join the fact table with dimension tables to create the final fact table
  val joinedDF = factTableDFWithSchema
    .join(productDimDF, Seq("product_id"), "left")
    .join(userDimDF, Seq("user_id"), "left")
    .join(dateDimDF, Seq("date_id"), "left")

  // Show the final joined DataFrame
  joinedDF.show(5)

  // ---- Save Dimension and Fact Tables ----

  // Save the product dimension to /tmp/dim_product
  productDimDFWithSchema.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .csv("/tmp/dim_product")

  // Save the user dimension to /tmp/dim_user
  userDimDFWithSchema.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .csv("/tmp/dim_user")

  // Save the date dimension to /tmp/dim_date
  dateDimDFWithSchema.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .csv("/tmp/dim_date")

  // Save the fact table to /tmp/fact_reviews
  factTableDFWithSchema.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .csv("/tmp/fact_reviews")

  // Save the final joined fact table to /tmp/sample_joined_fact_reviews
  joinedDF.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .csv("/tmp/sample_joined_fact_reviews")

  // Stop the Spark session
  spark.stop()

}