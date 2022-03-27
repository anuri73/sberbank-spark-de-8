package org.sberbank

import org.apache.spark.sql.{Dataset, SparkSession}

class MovieAnalyzer(val spark: SparkSession, src: String) {
  import spark.implicits._

  val movies: Dataset[String] =
    spark.read.option("inferSchema", value = true).textFile(src)

  private def ratings = movies
    .map(row => row.split("\t"))
    .map(row => (row(1).toInt, row(2).toInt))
    .cache()

  def movieRatingAmounts(movieId: Int): Seq[Long] = ratings
    .filter(row => row._1 == movieId)
    .groupByKey(_._2)
    .count()
    .collect()
    .toSeq
    .sortBy(_._1)
    .map(_._2)

  def ratingAmounts: Seq[Long] =
    ratings.groupByKey(_._2).count().collect().toSeq.sortBy(_._1).map(_._2)
}
