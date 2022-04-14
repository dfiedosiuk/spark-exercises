package P01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, udf}

object P01 extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val dept = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")

//  val splitUDF = udf{ (x: String, y: String) = x.spplit(y)
//
  val solution = dept.withColumn("split_values", expr("""${col("VALUES")}.split($Delimiter)""") )
}
