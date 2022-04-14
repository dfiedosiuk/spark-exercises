package P18

import org.apache.spark.sql.functions.{current_date, datediff, to_date}

object P18 extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val dates = Seq(
    "08/11/2015",
    "09/11/2015",
    "09/12/2015").toDF("date_string")

  val datesWithDate = dates.select(
    $"date_string",
    to_date($"date_string", "dd/mm/yyyy").as("to_date")
  )

  datesWithDate.withColumn("diff", datediff(current_date(),$"to_date")).show
}
