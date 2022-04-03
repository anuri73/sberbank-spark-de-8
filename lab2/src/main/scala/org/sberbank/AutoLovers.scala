package org.sberbank

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class AutoLovers(spark: SparkSession, path: String) {
  import spark.implicits._

  private val schema: StructType = StructType(
    Array(
      StructField("autousers", ArrayType(StringType, containsNull = false))
    )
  )

  def autoLovers: DataFrame =
    spark.read
      .schema(schema)
      .json(path)
      .select(explode($"autousers") as "auto_lovers")

  def totalAmount: Long = autoLovers.count()
}
