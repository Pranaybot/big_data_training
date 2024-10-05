import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object transformingData extends App {
  // Initialize Spark session
  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "TransformingData")
  sparkConfig.set("spark.master", "local[1]")
  val ss = SparkSession.builder().config(sparkConfig).getOrCreate()
  val productDf = ss.read.option("header", true).csv("C:/output/final_product_data/part-00000-b866cbdc-c8da-4823-bf74-1ada97cdc0a7-c000.csv")

  // 1. Add a new column called `product_size` based on `length`
  val productWithSizeDf = productDf.withColumn(
    "product_size",
    when(col("length") < 1000, "Small")
      .when(col("length").between(1000, 2000), "Medium")
      .when(col("length").between(2000, 3000), "Large")
      .otherwise("Large")
  )

  // Show the DataFrame with the new `product_size` column
  productWithSizeDf.show()

  // 2. Create a pivot based on `product_category` and `product_size`, and count the products
  val pivotDf = productWithSizeDf.groupBy("product_category")
    .pivot("product_size")
    .count()
    .na.fill(0)

  // Show the pivot result
  pivotDf.show()

  // 3. Use a window function to rank products by `length` within each `product_category`
  val windowSpec = Window.partitionBy("product_category").orderBy(col("length").desc)

  // Add a rank column
  val rankedDf = productWithSizeDf.withColumn("rank", row_number().over(windowSpec))

  // Filter to show the second-longest product in each category
  val secondLongestProductsDf = rankedDf.filter(col("rank") === 2)

  // Show the second-longest product for each category
  secondLongestProductsDf.select("product_category", "product_name", "length", "rank").show()

  // Stop the Spark session
  ss.stop()
}
