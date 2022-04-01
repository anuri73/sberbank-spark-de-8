package org.sberbank

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{
  col,
  count,
  regexp_extract,
  regexp_replace
}
import org.apache.spark.sql.types._

class LogAnalyzer(spark: SparkSession, path: String) {
  private val schema: StructType = StructType(
    Array(
      StructField("UID", LongType, nullable = true),
      StructField("Timestamp", DoubleType, nullable = true),
      StructField("URL", StringType, nullable = true)
    )
  )

  private val domainExp =
    """http(s?):\/\/(((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,6})"""
  private val wwwExp = """^(www\.)?(.+?)$"""

  def logs: DataFrame = spark.read
    .schema(schema)
    .options(Map("delimiter" -> "\t"))
    .option("encoding", "utf-8")
    .option("header", "true")
    .csv(path)

  def logsUrlFixed: DataFrame = logs
    .select("UID", "Timestamp", "URL")
    .filter(col("URL").isNotNull)
    .withColumn("URL", regexp_replace(col("URL"), "%(?![0-9a-fA-F]{2})", "%25"))
    .selectExpr(
      "UID",
      "Timestamp",
      "reflect('java.net.URLDecoder','decode', URL, 'utf-8') as URL"
    )
    .cache

  def domainLogs: DataFrame = logsUrlFixed
    .withColumn(
      "DOMAIN",
      regexp_extract(
        regexp_extract(col("URL"), domainExp, 2),
        wwwExp,
        2
      )
    )

  def totalLogAmount: Long = domainLogs.count()

  def withAutoLoverVisits(autoLovers: DataFrame): DataFrame =
    domainLogs
      .join(
        autoLovers,
        col("UID") === col("ID"),
        "left"
      )

  def autoLoversVisitAmount(autoLovers: DataFrame): Long =
    withAutoLoverVisits(autoLovers)
      .where(col("ID").isNotNull)
      .count()
}
