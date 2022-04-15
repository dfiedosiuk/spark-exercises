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
    .map {
      case row if row.startsWith("+") => null
      case row => row.split('|').tail
    }
    .na.drop
    .as[Seq[String]]
    .collect()


  val x = inputRows.toSeq.tail
    .map {
    case Seq(a, b, c) => (a, b, c)
  }.toDF(inputRows(0)(0), inputRows(0)(1), inputRows(0)(2))

  x.show
}
