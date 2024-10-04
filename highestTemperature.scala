import org.apache.spark.SparkContext

object highestTemperature extends App {
    val data = Seq(
      "s1,2016-01-01,20.5",
      "s2,2016-01-01,30.1",
      "s1,2016-01-02,60.2",
      "s2,2016-01-02,20.4",
      "s1,2016-01-03,55.5",
      "s2,2016-01-03,52.5"
    )

  val sc = new SparkContext("local[1]", "AppName")

  val rdd = sc.parallelize(data)

    val highestTemp = rdd
      .map(line => {
        val parts = line.split(",")
        (parts(0), parts(1), parts(2).toDouble) // (sensor id, date, temp)
      })
      .map(_._3) // extract temp
      .reduce(Math.max) // find the highest temp

    println(highestTemp) // Output: 60.2
}