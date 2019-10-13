package com.example.reporters

import com.example.model.{Answer, Question}
import com.example.reporters.common.util.HtmlUtil
import com.example.reporters.common.{Reporter, ReportUnit}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class CodeLenReporter(answersDs: Dataset[Answer], questionsDs: Dataset[Question]) extends Reporter {

  import com.example.session.SparkSessionHolder.spark.implicits._

  val reportData: ReportUnit = {
    val codeLenUdf = udf(HtmlUtil.codeLen _)
    val codeLenScoreDf = answersDs
      .withColumn("len", codeLenUdf($"body"))
      .select("score", "len")

    common.ReportUnit(codeLenScoreDf, "code_len_score")
  }
}
