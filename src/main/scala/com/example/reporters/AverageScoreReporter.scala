package com.example.reporters

import com.example.reporters.common.util.AverageUtil
import com.example.reporters.common.{Reporter, ReportUnit}
import org.apache.spark.sql.Dataset

case class AverageScoreReporter[T](dataset: Dataset[T], predicate: T => Boolean,
                                   reportName: String) extends Reporter {

  override val reportData: ReportUnit = {
    val result = AverageUtil.countMeanAndMedian(dataset, predicate, "score").toString
    import com.example.session.SparkSessionHolder.spark.implicits._;
    ReportUnit(Seq(result).toDF("avg"), reportName)
  }
}
