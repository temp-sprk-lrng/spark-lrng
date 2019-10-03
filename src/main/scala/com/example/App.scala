package com.example

import com.example.extractor.Extractor
import com.example.model.{Answer, Question, Tag}
import com.example.reporters.common.util.HtmlUtil
import com.example.reporters.{AverageScoreReporter, CodeLenReporter, DailyStatisticsReporter, ExpertsReporter, MajorTopicsReporter, QuotedReporter, SeasonalityReporter}
import com.example.session.SparkSessionHolder

object App extends App {

  val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
  val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")

  SparkSessionHolder.spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  SparkSessionHolder.spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", accessKeyId)
  SparkSessionHolder.spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", secretAccessKey)

  val questionsDs = Extractor.readCsv[Question]("s3a://spark-lrng-d.sei/data/Questions.csv")
  val answersDs = Extractor.readCsv[Answer]("s3a://spark-lrng-d.sei/data/Answers.csv")
  val tagsDs = Extractor.readCsv[Tag]("s3a://spark-lrng-d.sei/data/Tags.csv")

  val majorTopicsReporter = MajorTopicsReporter(tagsDs)
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