package com.example.reporters

import com.example.reporters.util.AverageUtil
import org.apache.spark.sql.Dataset

case class AverageScoreReporter[T](dataset: Dataset[T], predicate: T => Boolean, name: String) extends SimpleLogReporter {
  val col = "score"

  override def report(): Unit = {
  }

  override val reportData: Seq[ReportUnit] = {
    val result = AverageUtil.countMeanAndMedian(dataset, predicate, col).toString
    Seq(ReportUnit(result, name))
  }
}
