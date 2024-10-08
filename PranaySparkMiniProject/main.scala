package PranaySparkMiniProject // Adjust this based on your directory structure

import java.sql.{Connection, DriverManager, Statement}
import org.apache.spark.sql.SparkSession

object main extends App {

  val spark = SparkSession.builder
    .appName("Restaurant Challenge")
    .master("local")
    .getOrCreate()

  // JDBC connection properties
  val jdbcUrl = "jdbc:mysql://localhost:3306/challenge1"
  val connectionProps = new java.util.Properties()
  connectionProps.put("user", "ppentaparthy1")
  connectionProps.put("password", "&1!5.,oA")

  // Function to create tables
  def createTables(): Unit = {
    var connection: Connection = null
    var statement: Statement = null

    try {
      // Establishing the connection
      connection = DriverManager.getConnection(jdbcUrl, connectionProps)
      statement = connection.createStatement()

      // Create tables with SQL queries
      statement.executeUpdate(
        """CREATE TABLE IF NOT EXISTS total_spent (
          |customer_id VARCHAR(1),
          |total_spent DECIMAL(10,2)
          |);""".stripMargin)

      statement.executeUpdate(
        """CREATE TABLE IF NOT EXISTS days_visited (
          |customer_id VARCHAR(1),
          |days_visited INT
          |);""".stripMargin)

      statement.executeUpdate(
        """CREATE TABLE IF NOT EXISTS first_item_purchased (
          |customer_id VARCHAR(1),
          |product_name VARCHAR(50)
          |);""".stripMargin)

      statement.executeUpdate(
        """CREATE TABLE IF NOT EXISTS most_purchased_item (
          |product_name VARCHAR(50),
          |purchase_count INT
          |);""".stripMargin)

      statement.executeUpdate(
        """CREATE TABLE IF NOT EXISTS most_popular_item (
          |customer_id VARCHAR(1),
          |product_name VARCHAR(50)
          |);""".stripMargin)

      statement.executeUpdate(
        """CREATE TABLE IF NOT EXISTS first_item_after_member (
          |customer_id VARCHAR(1),
          |product_name VARCHAR(50),
          |first_purchase_date DATE
          |);""".stripMargin)

      statement.executeUpdate(
        """CREATE TABLE IF NOT EXISTS item_before_member (
          |customer_id VARCHAR(1),
          |product_name VARCHAR(50),
          |last_purchase_date DATE
          |);""".stripMargin)

      statement.executeUpdate(
        """CREATE TABLE IF NOT EXISTS total_items_before_member (
          |customer_id VARCHAR(1),
          |total_items INT,
          |total_spent DECIMAL(10,2)
          |);""".stripMargin)

      statement.executeUpdate(
        """CREATE TABLE IF NOT EXISTS points_per_customer (
          |customer_id VARCHAR(1),
          |total_points INT
          |);""".stripMargin)

      statement.executeUpdate(
        """CREATE TABLE IF NOT EXISTS points_after_membership (
          |customer_id VARCHAR(1),
          |total_points INT
          |);""".stripMargin)

      println("Tables created successfully.")
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // Clean up resources
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }

  // Call the createTables function
  createTables()

  // Create and run the data processing logic
  val processor = new dataProcessor(spark)

  // Call the method to perform all tasks
  processor.runAllQueries()
}
