package org.sberbank

import org.apache.spark.sql.SparkSession

import java.io.{BufferedWriter, File, FileWriter}

object MovieApp {

  val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val movieId = args(0).toInt

    val src = args(1)

    val outputName = args(2)

    try {

      val movieAnalyzer = new MovieAnalyzer(spark, src)

      val json = ujson.Obj(
        "hist_film" -> movieAnalyzer.movieRatingAmounts(movieId),
        "hist_all" -> movieAnalyzer.ratingAmounts
      )

      writeFile(outputName, json.toString())

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
