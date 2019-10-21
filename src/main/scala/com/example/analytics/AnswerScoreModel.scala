package com.example.analytics

import com.example.model.{Answer, Question}
import com.example.reporters.common.util.HtmlUtil
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer, VectorAssembler, Word2Vec}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset}

object AnswerScoreModel {

  def transformData(answerDs: Dataset[Answer], questionsDs: Dataset[Question]): DataFrame = {
    import com.example.session.SparkSessionHolder.spark.implicits._

    val containsLinkUdf = udf(HtmlUtil.containsLinks _)
    val codeLenUdf = udf(HtmlUtil.codeLen _)
    val bodyMapUdf = udf(HtmlUtil.tagFreeBody _)

    val questionScoreDf = questionsDs.select($"id".alias("question_id"), $"score".alias("question_score"))
    answerDs.join(questionScoreDf, $"partnerId" === $"question_id", "left")
      .withColumn("code_len", codeLenUdf($"body"))
      .withColumn("contains_link", containsLinkUdf($"body"))
      .withColumn("body_without_tags", bodyMapUdf($"body"))
      .select($"contains_link",
        $"code_len",
        $"question_score",
        $"body_without_tags".alias("body"),
        $"score".alias("label"))
  }

  def model(train: DataFrame,
            regressor: PipelineStage,
            gridBuilderUpdater: ParamGridBuilder => Unit): CrossValidatorModel = {
    val tokenized = new Tokenizer().setInputCol("body").setOutputCol("tokenized")

    val stopWordFree = new StopWordsRemover()
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
      .setInputCol("tokenized")
      .setOutputCol("free")

    val word2vec = new Word2Vec().setInputCol("free").setOutputCol("word2vec")

    val assembler = new VectorAssembler()
      .setInputCols(Array("contains_link", "code_len", "question_score", "word2vec"))
      .setOutputCol("features")

    val pipeline =
      new Pipeline().setStages(Array(tokenized, stopWordFree, word2vec, assembler.setHandleInvalid("skip"), regressor))

    val rmseEvaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val paramGridBuilder = new ParamGridBuilder()
      .addGrid(word2vec.vectorSize, Array(50, 100, 200))
    gridBuilderUpdater.apply(paramGridBuilder)

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(rmseEvaluator)
      .setEstimatorParamMaps(paramGridBuilder.build())

    cv.fit(train)
  }
}

