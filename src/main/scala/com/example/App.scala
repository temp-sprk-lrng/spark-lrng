package com.example

import com.example.analytics.AnswerScoreModel
import com.example.extractor.Extractor
import com.example.model.{Answer, Question}
import com.example.session.SparkSessionHolder
import com.example.util.CommonUtil
import org.apache.log4j.Logger
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression}
import org.apache.spark.ml.tuning.{CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.DataFrame

object App {
  private val log = Logger.getLogger(App.getClass)

  def init(args: Array[String]): Unit = {
    CommonUtil.baseFileLocation(args(0))
    SparkSessionHolder.init(args(1), args(2))
  }

  def logModelMetrics(model: CrossValidatorModel, testData: DataFrame): Unit = {
    val predictions = model.transform(testData)
    val rmse = new RegressionEvaluator().setMetricName("rmse").evaluate(predictions)
    val mae = new RegressionEvaluator().setMetricName("mae").evaluate(predictions)

    log.info("**********************************")
    log.info(s"rmse: $rmse")
    log.info(s"mae: $mae")
    log.info("**********************************")
  }

  def main(args: Array[String]): Unit = {
    init(args)
    val answersDs = Extractor.readCsv[Answer]("Answers.csv")
    val questionsDs = Extractor.readCsv[Question]("Questions.csv")

    val data = AnswerScoreModel.transformData(answersDs, questionsDs)
    val Array(train, test) = data.randomSplit(Array(.8, .2))

    val linearRegression = new LinearRegression()
    val linearParams = new ParamGridBuilder().addGrid(linearRegression.elasticNetParam, Array(0.3, 0.6, 0.8)).build()
    val linearModel = AnswerScoreModel.model(train, linearParams, linearRegression)


    val gbtRegressor = new GBTRegressor()
    val gbtParams = new ParamGridBuilder().addGrid(gbtRegressor.featureSubsetStrategy, Array("auto", "all")).build()
    val gbtModel = AnswerScoreModel.model(train, gbtParams, gbtRegressor)

    logModelMetrics(linearModel, test)
    logModelMetrics(gbtModel, test)
  }
}
