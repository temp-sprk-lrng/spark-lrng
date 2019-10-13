package com.example

import com.example.extractor.Extractor
import com.example.model.{Answer, Question, Tag}
import com.example.reporters._
import com.example.reporters.common.util.HtmlUtil
import com.example.session.SparkSessionHolder
import com.example.util.CommonUtil

object App {
  def init(args: Array[String]): Unit = {
    CommonUtil.baseFileLocation(args(0))
    SparkSessionHolder.init(args(1), args(2))
  }

  def main(args: Array[String]): Unit = {
    init(args)

    val questionsDs = Extractor.readCsv[Question]("Questions.csv")
    val answersDs = Extractor.readCsv[Answer]("Answers.csv")
    val tagsDs = Extractor.readCsv[Tag]("Tags.csv")

    val majorTopicsReporter = MajorTopicsReporter(tagsDs, 100)
    val expertsReporter = ExpertsReporter(questionsDs, answersDs, tagsDs, majorTopicsReporter.reportData.df)
    val reporters = Seq(
      DailyStatisticsReporter(questionsDs, answersDs),
      AverageScoreReporter(answersDs, (a: Answer) => HtmlUtil.containsLinks(a.body), "avg_contains_link_score"),
      AverageScoreReporter(answersDs, (a: Answer) => !HtmlUtil.containsLinks(a.body), "avg_not_contains_link_score"),
      AverageScoreReporter(answersDs, (a: Answer) => HtmlUtil.containsCode(a.body), "avg_contains_code_score"),
      AverageScoreReporter(answersDs, (a: Answer) => !HtmlUtil.containsCode(a.body), "avg_not_contains_code_score"),
      CodeLenReporter(answersDs, questionsDs),
      majorTopicsReporter,
      expertsReporter,
      QuotedReporter(answersDs, 100),
      SeasonalityReporter(questionsDs, answersDs)
    )

    reporters.foreach(_.report)
  }
}
