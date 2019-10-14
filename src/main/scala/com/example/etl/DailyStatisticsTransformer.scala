package com.example.etl

import com.example.model.{Answer, Question}
import com.example.etl.common.BiTransformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, Dataset}

object DailyStatisticsTransformer extends BiTransformer[Question, Answer] {

  override def transform(questionsDs: Dataset[Question], answersDs: Dataset[Answer]): DataFrame = {
    import com.example.session.SparkSessionHolder.spark.implicits._
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
    unionDf
      .groupBy("creationDate")
      .agg(
        countDistinct("ownerUserId").alias("unique_users"),
        count("questionId").alias("number_of_questions"),
        count("answerId").alias("number_of_answers")
      )
  }
}
