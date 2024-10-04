import org.apache.spark.SparkContext

object temperaturesAbove50 extends App {

  // Assuming SparkContext `sc` is already created
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

  val counts = rdd
    .map(line => {
      val parts = line.split(",")
      (parts(0), parts(2).toDouble) // (sensor id, temp)
    })
    .filter { case (_, temp) => temp > 50 } // filter temps greater than 50
    .map { case (sensorId, _) => (sensorId, 1) } // map to (sensorId, 1)
    .reduceByKey(_ + _) // count occurrences for each sensor

  counts.collect().foreach { case (sensorId, count) =>
    println(s"$count, $sensorId") // Output in specified format
  }
}
