package com.example.etl.common.util

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.avg

object AverageUtil {

  def countMean[T](dataset: Dataset[T], predicate: T => Boolean, col: String): Double = {
    dataset
      .filter(predicate)
      .select(avg(col))
      .first.getDouble(0)
  }

  def countMedian[T](dataset: Dataset[T],
                     predicate: T => Boolean,
                     col: String,
                     error: Double = 0.1): Double = {
    dataset.filter(predicate).stat.approxQuantile(col, Array(0.5), error)(0)
  }

  def countMeanAndMedian[T](dataset: Dataset[T], predicate: T => Boolean, col: String): (Double, Double) = {
    (countMean(dataset, predicate, col), countMedian(dataset, predicate, col))
  }
}
