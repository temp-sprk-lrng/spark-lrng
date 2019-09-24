package com.example.reporters

import com.example.model.{Answer, Question, Tag}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

case class TopTopicsReporter(questionsDs: Dataset[Question],
                             answersDs: Dataset[Answer],
                             tagsDs: Dataset[Tag]) extends DfLogReporter {

  import com.example.session.SparkSessionHolder.spark.implicits._

  override val reportData: Seq[ReportDfUnit] = {
    val majorTopics = getMajorTopics
    val experts = getExperts(majorTopics)
    Seq(
      ReportDfUnit(majorTopics, "majot_topics"),
      ReportDfUnit(experts, "experts")
    )
  }

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

  private def getMajorTopics = {
    tagsDs
      .groupBy("tag")
      .agg(count("id").alias("amount"))
      .orderBy(desc("amount"))
      .limit(100)
      .cache
  }
}
