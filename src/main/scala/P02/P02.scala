package P02

import org.apache.spark.sql.{SparkSession, functions}

object P02 extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    (1, "MV1"),
    (1, "MV2"),
    (2, "VPV"),
    (2, "Others")).toDF("id", "value")

  input.printSchema()

  val priorities = Seq(
    "MV1",
    "MV2",
    "VPV",
    "Others").zipWithIndex.toDF("name", "idx")

  val matching = input
    .join(priorities)
    .where($"value" === $"name")
    .groupBy("id")
    .agg(functions.min("idx"))

  val solution = matching
    .join(priorities)
    .where($"idx" === $"rank")
    .select("id", "name")

  solution.show


}
