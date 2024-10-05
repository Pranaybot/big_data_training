import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CleaningData extends App {
  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "DataframeDemo")
  sparkConfig.set("spark.master", "local[1]")
  val ss = SparkSession.builder().config(sparkConfig).getOrCreate()

  val ddlSchema =
    """
      product_number STRING,
      product_name STRING,
      product_category STRING,
      product_scale STRING,
      product_manufacturer STRING,
      product_description STRING,
      length DOUBLE,
      width DOUBLE,
      height DOUBLE
  """

  val productDf = ss.read.option("header", true).schema(ddlSchema).csv("C:/products.csv")

  // Show the schema to debug any issues
  productDf.printSchema()

  // Check for unique values in length and width to identify bad data
  productDf.select("length").distinct().show(100)  // Check distinct values in length
  productDf.select("width").distinct().show(100)   // Check distinct values in width

  // 1. Check for outliers in length and width columns using the IQR method
  def detectOutliers(column: String) = {
    val stats = productDf.describe(column).collect()
    val q1 = 0.25
    val q3 = 0.75
    val iqr = q3 - q1
    val lowerBound = q1 - 1.5 * iqr
    val upperBound = q3 + 1.5 * iqr

    productDf.filter(col(column) < lowerBound || col(column) > upperBound)
  }

  val lengthOutliers = detectOutliers("length")
  val widthOutliers = detectOutliers("width")

  println("Length Outliers:")
  lengthOutliers.show()

  println("Width Outliers:")
  widthOutliers.show()

  // 2. Split product_number into storeid and productid
  val splitProductDf = productDf.withColumn("storeid", split(col("product_number"), "_").getItem(0))
    .withColumn("productid", split(col("product_number"), "_").getItem(1))

  // 3. Extract year from product_name (assuming year is the last part of the product_name)
  val finalProductDf = splitProductDf
    .withColumn("year", regexp_extract(col("product_name"), "\\d{4}$", 0))
    .filter(col("product_category").isNotNull)


  // Show the transformed DataFrame
  finalProductDf.show()

  // Write finalProductDf to a CSV file
  finalProductDf.write
    .option("header", "true")  // Include header in the output file
    .mode("overwrite")         // Overwrite the file if it already exists
    .csv("C:/output/final_product_data")  // Specify the output path

  // Stop Spark session
  ss.stop()
}