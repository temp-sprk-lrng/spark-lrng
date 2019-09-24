package com.example.reporters

import com.example.model.{Answer, Question}
import com.example.reporters.util.HtmlUtil
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class CodeLenReporter(answersDs: Dataset[Answer], questionsDs: Dataset[Question]) extends DfLogReporter {

  import com.example.session.SparkSessionHolder.spark.implicits._

  val reportData: Seq[ReportDfUnit] = {
    val codeLenDf = getCodeLenDf
    val codeLenUdf = udf(HtmlUtil.codeLen _)
    val codeLenScoreDf = answersDs
      .withColumn("len", codeLenUdf($"body"))
      .select("score", "len")

    Seq(
      ReportDfUnit(codeLenDf.toDF, "code_len"),
      ReportDfUnit(codeLenScoreDf, "code_len_score")
    )
  }

  private def getCodeLenDf: Dataset[Int] = {
    answersDs.select(col("body"))
      .unionAll(questionsDs.select(col("body")))
      .map(r => HtmlUtil.codeLen(r(0).toString))
  }
}
