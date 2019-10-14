package com.example.etl

import com.example.etl.common.TriTransformer
import com.example.model.{Answer, Question, Tag}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

case class ExpertsTransformer(majorTopics: DataFrame) extends TriTransformer[Question, Answer, Tag] {

  import com.example.session.SparkSessionHolder.spark.implicits._

  private def totalTopicScore(majorTopics: Dataset[Row],
                              tagsDs: Dataset[Tag],
                              questionsDs: Dataset[Question]): DataFrame = {
    majorTopics
      .join(tagsDs, Seq("tag"), "left_outer")
      .join(questionsDs, Seq("id"), "inner")
      .groupBy("tag", "ownerUserId")
      .agg(sum("score").alias("total_score"))
  }

  override def transform(questionsDs: Dataset[Question],
                         answersDs: Dataset[Answer],
                         tagsDs: Dataset[Tag]): DataFrame = {
    val overTag = Window.partitionBy($"tag").orderBy($"total_score".desc)
    val ranked = totalTopicScore(majorTopics, tagsDs, questionsDs).withColumn("rank", dense_rank.over(overTag))
    ranked
      .select("tag", "total_score", "ownerUserId")
      .where($"rank" <= 1)
      .orderBy(desc("total_score"))
  }
}


