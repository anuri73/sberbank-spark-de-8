package org.sberbank

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit, _}

import java.io.{BufferedWriter, File, FileWriter}

object App {

  private val precision = 15

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
    import spark.implicits._

    val autoLoversSourceFile = args(0)

    val logsSourceFile = args(1)

    val resultDestinationFile = args(2)

    try {
      val autoLovers = new AutoLovers(spark, autoLoversSourceFile).autoLovers

      val logs = new LogAnalyzer(spark, logsSourceFile)

      val totalAutoLoversVisitedAmount = logs.autoLoversVisitAmount(autoLovers)

      val topDomains = logs
        .withAutoLoverVisits(autoLovers)
        .select(
          $"domain",
          $"auto_lovers",
          $"uid"
        )
        .groupBy("domain")
        .agg(
          count("auto_lovers") as "auto_lovers_visited_amount",
          count("uid") as "visited_amount"
        )
        .withColumn("numerator", pow($"auto_lovers_visited_amount", 2))
        .withColumn("denumerator", $"visited_amount" * lit(totalAutoLoversVisitedAmount))
        .where($"numerator" > 0.0 and $"denumerator" > 0.0)
        .withColumn(
          "relevance",
          format_number(
            $"numerator" / $"denumerator",
            precision
          )
        )
        .select("domain", "relevance")
        .orderBy($"relevance".desc, $"domain".asc)
        .limit(200)

      writeFile(
        resultDestinationFile,
        topDomains
          .map(_.mkString("\t"))
          .collect()
          .mkString("\n")
      )

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
