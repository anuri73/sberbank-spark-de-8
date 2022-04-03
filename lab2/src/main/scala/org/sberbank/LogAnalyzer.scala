package org.sberbank

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.sberbank.udf.URL

class LogAnalyzer(spark: SparkSession, path: String) {
  import spark.implicits._

  private val schema: StructType = StructType(
    Array(
      StructField("uid", LongType, nullable = true),
      StructField("timestamp", DoubleType, nullable = true),
      StructField("url", StringType, nullable = true)
    )
  )

  def logs: DataFrame = spark.read
    .schema(schema)
    .options(Map("delimiter" -> "\t"))
    .option("encoding", "utf-8")
    .option("header", "false")
    .csv(path)

  def domainLogs: DataFrame = logs
    .withColumn("url", URL.decode($"url"))
    .filter($"url".startsWith("http"))
    .withColumn("domain", URL.getDomain($"url"))
    .drop("url")
    .where($"domain".isNotNull and $"uid".isNotNull)

  def totalLogAmount: Long = domainLogs.count()

  def withAutoLoverVisits(autoLovers: DataFrame): DataFrame =
    domainLogs
      .join(
        autoLovers,
        $"uid" === $"auto_lovers",
        "left"
      )

  def autoLoversVisitAmount(autoLovers: DataFrame): Long =
    withAutoLoverVisits(autoLovers)
      .where($"auto_lovers".isNotNull)
      .count()
}
