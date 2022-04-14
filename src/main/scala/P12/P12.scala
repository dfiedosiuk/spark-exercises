package P12

import org.apache.spark.sql.SparkSession

object P12 extends App {

  val spark = SparkSession
  .builder()
  .master("local[*]")
  .getOrCreate()

  import spark.implicits._

  val input = spark.read.text("src/main/scala/P12/input.txt")

  val inputRows = input
    .select("value")
    .as[String]
    .map{
    case row if row.startsWith("+") => null
    case row => row.split('|').tail
    }
    .na.drop
    .as[Seq[String]]
    .collect()

//    .show(false)

  inputRows.foreach(println)
}
