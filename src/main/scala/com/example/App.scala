package com.example

import com.example.analytics.AnswerScoreModel
import com.example.extractor.Extractor
import com.example.model.{Answer, Question}
import com.example.session.SparkSessionHolder
import com.example.util.CommonUtil
import org.apache.spark.ml.regression.LinearRegression

object App {
  def init(args: Array[String]): Unit = {
    CommonUtil.baseFileLocation(args(0))
    SparkSessionHolder.init(args(1), args(2))
  }

  def main(args: Array[String]): Unit = {
    init(args)
    val answersDs = Extractor.readCsv[Answer]("Answers.csv")
    val questionsDs = Extractor.readCsv[Question]("Questions.csv")

    val data = AnswerScoreModel.transformData(answersDs, questionsDs)
    val Array(train, test) = data.randomSplit(Array(.8, .2))
    val model = AnswerScoreModel.model(train, new LinearRegression(), b => {})
    val predictions = model.transform(test)

    predictions.show
  }
}
