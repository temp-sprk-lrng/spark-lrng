package com.example

import com.example.etl._
import com.example.etl.common.util.{EtlUtil, HtmlUtil}
import com.example.etl.defenition.{EtlBiDefinition, EtlDefinition, EtlTriDefinition}
import com.example.model.{Answer, Question, Tag}
import com.example.session.SparkSessionHolder
import com.example.util.CommonUtil

object App {
  def init(args: Array[String]): Unit = {
    CommonUtil.baseFileLocation(args(0))
    SparkSessionHolder.init(args(1), args(2))
  }

  def main(args: Array[String]): Unit = {
    init(args)

    val questionsDs = EtlUtil.readCsv[Question]("Questions.csv")
    val answersDs = EtlUtil.readCsv[Answer]("Answers.csv")
    val tagsDs = EtlUtil.readCsv[Tag]("Tags.csv")

    val majorTopicsEtl =  EtlDefinition(
      sourceDF = tagsDs,
      transform = MajorTopicsTransformer(100).transform,
      write = df => EtlUtil.writeToS3(df, "major_topics")
    )
    val majorTopicsDf = majorTopicsEtl.transform(majorTopicsEtl.sourceDF)
    majorTopicsEtl.write(majorTopicsDf);

    val reporters = Seq(
      EtlBiDefinition(
        sourceDf1 = questionsDs,
        sourceDf2 = answersDs,
        transformer = DailyStatisticsTransformer.transform,
        write = df => EtlUtil.writeToS3(df, "daily_statistics")
      ),
      EtlDefinition(
        sourceDF = answersDs,
        transform = AverageScoreTransformer((a: Answer) => HtmlUtil.containsLinks(a.body)).transform,
        write = df => EtlUtil.writeToS3(df, "avg_score_with_hrefs")
      ),
      EtlDefinition(
        sourceDF = answersDs,
        transform = AverageScoreTransformer((a: Answer) => !HtmlUtil.containsLinks(a.body)).transform,
        write = df => EtlUtil.writeToS3(df, "avg_score_without_hrefs")
      ),
      EtlDefinition(
        sourceDF = answersDs,
        transform = AverageScoreTransformer((a: Answer) => HtmlUtil.containsCode(a.body)).transform,
        write = df => EtlUtil.writeToS3(df, "avg_score_with_code")
      ),
      EtlDefinition(
        sourceDF = answersDs,
        transform = AverageScoreTransformer((a: Answer) => !HtmlUtil.containsCode(a.body)).transform,
        write = df => EtlUtil.writeToS3(df, "avg_score_without_code")
      ),
      EtlTriDefinition(
        sourceDF1 = questionsDs,
        sourceDF2 = answersDs,
        sourceDF3 = tagsDs,
        transformer = ExpertsTransformer(majorTopicsDf).transform,
        write =  df => EtlUtil.writeToS3(df, "experts")
      )

    )

    reporters.foreach(_.process())
  }
}
