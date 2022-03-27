package org.sberbank

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class MovieSuite extends munit.FunSuite {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Lab1")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val movieAnalyzer =
    new MovieAnalyzer(spark, "src/test/resources/ml-100k/u.data")

  test("Groundhog Day rating amount must be equal to specific value") {
    assertEquals(
      movieAnalyzer.movieRatingAmounts(202),
      ArrayBuffer[Long](4, 21, 76, 115, 64)
    )
  }

  test("All rating amount must be equal to specific value") {
    assertEquals(
      movieAnalyzer.ratingAmounts,
      ArrayBuffer[Long](6110, 11370, 27145, 34174, 21201)
    )
  }
}
