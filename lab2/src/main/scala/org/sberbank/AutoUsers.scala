package org.sberbank

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class AutoUsers(spark: SparkSession, path: String) {

  private val schema: StructType = StructType(
    Array(
      StructField("autousers", ArrayType(StringType, containsNull = false))
    )
  )

  def autoLovers: DataFrame =
    spark.read
      .schema(schema)
      .json(path)
      .select(explode(col("autousers")) as "ID")
      .distinct()
      .cache()

  def totalAmount: Long = autoLovers.count()
}
