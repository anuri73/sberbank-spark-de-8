package org.sberbank

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.io.{BufferedWriter, File, FileWriter}

object App {

  val spark: SparkSession = SparkSession
    .builder()
//    .master("local[*]")
//    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  val tsvWithHeaderOptions: Map[String, String] = Map(
    ("delimiter", "\t"),
    ("header", "false")
  )

  def main(args: Array[String]): Unit = {

    val autoUsersPath = args(0)

    val logPath = args(1)

    val outputPath = args(2)

    try {
      val autoLovers = new AutoUsers(spark, autoUsersPath).autoLovers

      val logs = new LogAnalyzer(spark, logPath)

      val totalLogAmount = logs.totalLogAmount

      val autoLoverVisitLogAmount = logs.autoLoversVisitAmount(autoLovers)

      logs
        .withAutoLoverVisits(autoLovers)
        .select("DOMAIN", "ID", "UID")
        .groupBy("DOMAIN")
        .agg(
          count("ID") as "AUTO_LOVER_VISITED_AMOUNT",
          count("UID") as "TOTAL_VISITED_AMOUNT"
        )
        .withColumn(
          "RELEVANCE",
          pow(col("AUTO_LOVER_VISITED_AMOUNT") / lit(totalLogAmount), 2)
            /
              (
                (col("TOTAL_VISITED_AMOUNT") / lit(totalLogAmount))
                  *
                    (lit(autoLoverVisitLogAmount) / lit(totalLogAmount))
              )
        )
        .orderBy(desc("RELEVANCE"), asc("DOMAIN"))
        .select(
          col("DOMAIN"),
          format_number(col("RELEVANCE"), 15)
        )
        .limit(200)
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .options(tsvWithHeaderOptions)
        .csv(outputPath)

    } finally {
      spark.close
    }
  }

  def writeFile(filename: String, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s)
    bw.close()
  }
}
