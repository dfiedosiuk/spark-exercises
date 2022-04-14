package P39

import org.apache.spark.sql.SparkSession

object P39 extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    (1, "one"),
    (2, "two")
  ).toDF("id","names")

  val tableName = args.head
  val sqlQuery = args.tail.mkString(" ")

  input.createOrReplaceTempView(tableName)

  spark.sql(sqlQuery).show
}
