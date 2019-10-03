package com.example.reporters

import com.example.reporters.common.util.AverageUtil
import com.example.reporters.common.{ReportUnit, SimpleLogReporter}
import org.apache.spark.sql.Dataset

case class AverageScoreReporter[T](dataset: Dataset[T], predicate: T => Boolean, name: String) extends SimpleLogReporter {
  val col = "score"

  override def report(): Unit = {
  }

  override val reportData: ReportUnit = {
    val result = AverageUtil.countMeanAndMedian(dataset, predicate, col).toString
    ReportUnit(result, name)
  }
}
