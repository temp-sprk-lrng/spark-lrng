package com.example.etl

import com.example.model.{Answer, Question}
import com.example.etl.common.BiTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

object SeasonalityTransformer extends BiTransformer[Question, Answer] {

  override def transform(questionsDs: Dataset[Question], answersDs: Dataset[Answer]): DataFrame = {
    import com.example.session.SparkSessionHolder.spark.implicits._
    val selectCols = Seq($"score", month($"creationDate").alias("month"))
    questionsDs
      .select(selectCols: _*)
      .union(answersDs.select(selectCols: _*))
      .groupBy("month")
      .agg(mean("score").alias("avg"))
      .orderBy("month")
  }
}
