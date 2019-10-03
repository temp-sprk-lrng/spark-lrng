package com.example.reporters

import com.example.model.{Answer, Question}
import com.example.reporters.common.util.HtmlUtil
import com.example.reporters.common.{DfLogReporter, ReportDfUnit}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class CodeLenReporter(answersDs: Dataset[Answer], questionsDs: Dataset[Question]) extends DfLogReporter {

  import com.example.session.SparkSessionHolder.spark.implicits._

  val reportData: ReportDfUnit = {
    val codeLenUdf = udf(HtmlUtil.codeLen _)
    val codeLenScoreDf = answersDs
      .withColumn("len", codeLenUdf($"body"))
      .select("score", "len")

    common.ReportDfUnit(codeLenScoreDf, "code_len_score")
  }
}
