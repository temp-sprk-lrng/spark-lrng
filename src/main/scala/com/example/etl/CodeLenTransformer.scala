package com.example.etl

import com.example.model.{Answer, Question}
import com.example.etl.common.BiTransformer
import com.example.etl.common.util.HtmlUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}


object CodeLenTransformer extends BiTransformer[Answer, Question] {

  override def transform(answersDs: Dataset[Answer], questionsDs: Dataset[Question]): DataFrame = {
    import com.example.session.SparkSessionHolder.spark.implicits._
    val codeLenUdf = udf(HtmlUtil.codeLen _)
    answersDs
      .withColumn("len", codeLenUdf($"body"))
      .select("score", "len")
  }

}
