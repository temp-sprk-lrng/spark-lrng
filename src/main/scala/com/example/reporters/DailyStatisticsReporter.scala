package com.example.reporters

import com.example.model.{Answer, Question}
import com.example.reporters.common.{DfLogReporter, ReportDfUnit}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType


case class DailyStatisticsReporter(questionsDs: Dataset[Question],
                                   answersDs: Dataset[Answer]) extends DfLogReporter {

  import com.example.session.SparkSessionHolder.spark.implicits._

  val reportData: ReportDfUnit = {
    val questionNaFreeDs = questionsDs
      .withColumn("creationDate", $"creationDate".cast(DateType))
      .na.drop(Seq("creationDate"))
    val answersNaFreeDs = answersDs
      .withColumn("creationDate", $"creationDate".cast(DateType))
      .na.drop(Seq("creationDate"))

    val questionCols = Seq(
      lit(null).alias("answerId"),
      $"ownerUserId",
      $"creationDate".cast(DateType),
      $"id".alias("questionId")
    )
    val answerCols = Seq(
      $"id".alias("answerId"),
      $"ownerUserId",
      $"creationDate".cast(DateType),
      lit(null).alias("questionId")
    )
    val unionDf = questionNaFreeDs.select(questionCols: _*).unionByName(answersNaFreeDs.select(answerCols: _ *))
    val dailyStatisticsDf = unionDf
      .groupBy("creationDate")
      .agg(
        countDistinct("ownerUserId").alias("unique_users"),
        count("questionId").alias("number_of_questions"),
        count("answerId").alias("number_of_answers")
      )

    common.ReportDfUnit(dailyStatisticsDf, "daily_statistics")
  }
}
