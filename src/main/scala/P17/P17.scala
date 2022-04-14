package P17

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._

object P17 extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val nums = Seq(Seq(1,2,3)).toDF("nums")

  nums.withColumn("num", explode(col("nums"))).show


}
