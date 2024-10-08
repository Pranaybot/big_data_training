package PranaySparkMiniProject

import org.apache.spark.sql.DataFrame

object mySQLUtils {

  def writeToMySQL(df: DataFrame, tableName: String): Unit = {
    val jdbcUrl = "jdbc:mysql://localhost:3306/challenge1"
    val connectionProps = new java.util.Properties()
    connectionProps.put("user", "ppentaparthy1")  // Update with your username
    connectionProps.put("password", "&1!5.,oA")  // Update with your password

    df.write
      .mode("overwrite")  // Or use "append"
      .jdbc(jdbcUrl, tableName, connectionProps)
  }

}
