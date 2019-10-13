package com.example.reporters

import com.example.model.{Answer, Question}
import com.example.reporters.common.{Reporter, ReportUnit}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class SeasonalityReporter(questionsDs: Dataset[Question], answersDs: Dataset[Answer]) extends Reporter {

  import com.example.session.SparkSessionHolder.spark.implicits._

  override val reportData: ReportUnit = {
    val selectCols = Seq($"score", month($"creationDate").alias("month"))
    val seasonalityDf = questionsDs
      .select(selectCols: _*)
      .union(answersDs.select(selectCols: _*))
      .groupBy("month")
      .agg(mean("score").alias("avg"))
      .orderBy("month")

    common.ReportUnit(seasonalityDf, "seasonality")
  }
}
