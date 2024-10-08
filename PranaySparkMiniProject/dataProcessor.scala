package PranaySparkMiniProject

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import PranaySparkMiniProject.mySQLUtils.writeToMySQL

class dataProcessor(spark: SparkSession) {

  import spark.implicits._

  // Reading tables from MySQL
  val salesDF: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/challenge1", "sales", new java.util.Properties())
  val menuDF: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/challenge1", "menu", new java.util.Properties())
  val membersDF: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/challenge1", "members", new java.util.Properties())

  // Function to run all queries and write the results to MySQL
  def runAllQueries(): Unit = {
    runTotalSpent()
    runDaysVisited()
    runFirstItemPurchased()
    runMostPurchasedItem()
    runMostPopularItem()
    runFirstItemAfterMembership()
    runItemBeforeMembership()
    runTotalItemsBeforeMembership()
    runPointsPerCustomer()
    runPointsAfterMembership()
  }

  // Question 1: Total amount spent by each customer
  def runTotalSpent(): Unit = {
    val totalSpentDF = salesDF
      .join(menuDF, "product_id")
      .groupBy("customer_id")
      .agg(sum(col("price")).alias("total_spent"))

    writeToMySQL(totalSpentDF, "total_spent")
  }

  // Question 2: Days visited by each customer
  def runDaysVisited(): Unit = {
    val daysVisitedDF = salesDF
      .groupBy("customer_id")
      .agg(countDistinct("order_date").alias("days_visited"))

    writeToMySQL(daysVisitedDF, "days_visited")
  }

  // Question 3: First item purchased by each customer
  def runFirstItemPurchased(): Unit = {
    val firstItemDF = salesDF
      .join(menuDF, "product_id")
      .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy("order_date")))
      .filter("rank = 1")
      .select("customer_id", "product_name")

    writeToMySQL(firstItemDF, "first_item_purchased")
  }

  // Question 4: Most purchased item and the number of times it was purchased
  def runMostPurchasedItem(): Unit = {
    val mostPurchasedItemDF = salesDF
      .groupBy("product_id")
      .agg(count("*").alias("purchase_count"))
      .join(menuDF, Seq("product_id"))
      .select("product_name", "purchase_count")
      .orderBy(col("purchase_count").desc)
      .limit(1)

    writeToMySQL(mostPurchasedItemDF, "most_purchased_item")
  }

  // Question 5: Most popular item for each customer
  def runMostPopularItem(): Unit = {
    val mostPopularItemDF = salesDF
      .groupBy("customer_id", "product_id")
      .agg(count("*").alias("purchase_count"))
      .join(menuDF, "product_id")
      .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy(col("purchase_count").desc)))
      .filter("rank = 1")
      .select("customer_id", "product_name")

    writeToMySQL(mostPopularItemDF, "most_popular_item")
  }

  // Question 6: First item purchased after becoming a member
  def runFirstItemAfterMembership(): Unit = {
    val firstItemAfterMember = spark.sql("""
      SELECT s.customer_id, m.product_name, MIN(s.order_date) AS first_purchase_date
      FROM sales s
      JOIN menu m ON s.product_id = m.product_id
      JOIN members mem ON s.customer_id = mem.customer_id
      WHERE s.order_date > mem.join_date
      GROUP BY s.customer_id, m.product_name
      ORDER BY first_purchase_date
    """)
    writeToMySQL(firstItemAfterMember, "first_item_after_member")
  }

  // Question 7: Item purchased just before becoming a member
  def runItemBeforeMembership(): Unit = {
    val itemBeforeMember = spark.sql("""
      SELECT s.customer_id, m.product_name, MAX(s.order_date) AS last_purchase_date
      FROM sales s
      JOIN menu m ON s.product_id = m.product_id
      JOIN members mem ON s.customer_id = mem.customer_id
      WHERE s.order_date < mem.join_date
      GROUP BY s.customer_id, m.product_name
      ORDER BY last_purchase_date DESC
    """)
    writeToMySQL(itemBeforeMember, "item_before_member")
  }

  // Question 8: Total items and amount spent before becoming a member
  def runTotalItemsBeforeMembership(): Unit = {
    val totalBeforeMember = spark.sql("""
      SELECT s.customer_id, COUNT(s.product_id) AS total_items, SUM(menu.price) AS total_spent
      FROM sales s
      JOIN menu ON s.product_id = menu.product_id
      JOIN members mem ON s.customer_id = mem.customer_id
      WHERE s.order_date < mem.join_date
      GROUP BY s.customer_id
    """)
    writeToMySQL(totalBeforeMember, "total_items_before_member")
  }

  // Question 9: Points earned by each customer (with sushi 2x multiplier)
  def runPointsPerCustomer(): Unit = {
    val pointsDF = spark.sql("""
      SELECT s.customer_id, SUM(
        CASE
          WHEN m.product_name = 'sushi' THEN 2 * m.price * 10
          ELSE m.price * 10
        END
      ) AS total_points
      FROM sales s
      JOIN menu m ON s.product_id = m.product_id
      GROUP BY s.customer_id
    """)
    writeToMySQL(pointsDF, "points_per_customer")
  }

  // Question 10: Points after joining with 2x multiplier for first week
  def runPointsAfterMembership(): Unit = {
    val pointsAfterJoinDF = spark.sql("""
      SELECT s.customer_id, SUM(
        CASE
          WHEN s.order_date BETWEEN mem.join_date AND mem.join_date + INTERVAL 6 DAY THEN m.price * 20
          WHEN m.product_name = 'sushi' THEN m.price * 20
          ELSE m.price * 10
        END
      ) AS total_points
      FROM sales s
      JOIN menu m ON s.product_id = m.product_id
      JOIN members mem ON s.customer_id = mem.customer_id
      WHERE s.order_date <= '2021-01-31'
      GROUP BY s.customer_id
    """)

    writeToMySQL(pointsAfterJoinDF, "points_after_membership")
  }
}
