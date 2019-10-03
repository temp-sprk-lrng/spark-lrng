package com.example.reporters

import com.example.model.{Answer, Question, Tag}
import com.example.reporters.common.{DfLogReporter, ReportDfUnit}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

case class ExpertsReporter(questionsDs: Dataset[Question],
                           answersDs: Dataset[Answer],
                           tagsDs: Dataset[Tag],
                           majorTopics: DataFrame) extends DfLogReporter {

  import com.example.session.SparkSessionHolder.spark.implicits._

  override val reportData: ReportDfUnit = common.ReportDfUnit(getExperts(majorTopics), "experts")

  private def getExperts(majorTopics: DataFrame) = {
    val overTag = Window.partitionBy($"tag").orderBy($"total_score".desc)
    val ranked = getTotalTopicsScoreDf(majorTopics).withColumn("rank", dense_rank.over(overTag))
    ranked
      .select("tag", "total_score", "ownerUserId")
      .where($"rank" <= 1)
      .orderBy(desc("total_score"))
  }

  private def getTotalTopicsScoreDf(majorTopics: Dataset[Row]) = {
    majorTopics
      .join(tagsDs, Seq("tag"), "left_outer")
      .join(questionsDs, Seq("id"), "inner")
      .groupBy("tag", "ownerUserId")
      .agg(sum("score").alias("total_score"))
  }

}


