package P13

object P13 extends App{
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    Seq("a","b","c"),
    Seq("X","Y","Z")
  ).toDF

  val arraySize = input.as[Seq[String]].head.length

  (0 until arraySize).foldLeft(input){
    (result, idx) =>
      result.withColumn(s"$idx", $"value"(idx))}
    .drop("value")
    .show
}
