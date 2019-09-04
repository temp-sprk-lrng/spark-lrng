// Databricks notebook source
import java.sql.Timestamp
import java.net.{URI, URISyntaxException}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SaveMode}
import org.apache.spark.sql.types.DateType
import org.jsoup.Jsoup
import org.jsoup.select.Elements

import reflect.runtime.universe.TypeTag

import spark.implicits._ 


// COMMAND ----------

def readDs[T <: Product : TypeTag](filename: String): Dataset[T] = {
    spark.read
      .option("multiLine", true)
      .option("header", true)
      .schema(Encoders.product[T].schema)
      .option("escape", "\"")
      .csv("/FileStore/tables/" + filename)
      .na.drop("all")
      .as[T]
      .cache()
}

// COMMAND ----------

case class Question(id: Long,
                    ownerUserId: Long,
                    creationDate: Timestamp,
                    score: Long,
                    title: String,
                    body: String)

val questionsDs = readDs[Question]("Questions.csv")


// COMMAND ----------

case class Answer(id: Long,
                  ownerUserId: Long,
                  creationDate: Timestamp,
                  partnerId: Long,
                  score: Long,
                  body: String)

val answersDs =  readDs[Answer]("Answers.csv")

// COMMAND ----------

case class Tag(id: Long,
               tag: String)

val tagsDs = readDs[Tag]("Tags.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC Collect daily statistics (number of unique users posted, number of questions and answers) into a separate table and export to a Parquet file.

// COMMAND ----------

val questionNaFreeDs = questionsDs
      .withColumn("creationDate", $"creationDate".cast(DateType))
      .na.drop(Seq("creationDate"))
val answersNaFreeDs = answersDs
  .withColumn("creationDate", $"creationDate".cast(DateType))
  .na.drop(Seq("creationDate"))

val questionCols = Seq(
  lit(null).alias("answerId"),
  $"ownerUserId",
  $"creationDate".cast(DateType),
  $"id".alias("questionId")
)
val answerCols = Seq(
  $"id".alias("answerId"),
  $"ownerUserId",
  $"creationDate".cast(DateType),
  lit(null).alias("questionId")
)
val unionDf = questionNaFreeDs.select(questionCols: _*).unionByName(answersNaFreeDs.select(answerCols: _ *))
val dailyStatisticsDf = unionDf
  .groupBy("creationDate")
  .agg(
  countDistinct("ownerUserId").alias("unique_users"),
  count("questionId").alias("number_of_questions"),
  count("answerId").alias("number_of_answers")
)

dailyStatisticsDf.write.mode(SaveMode.Overwrite).parquet("/FileStore/tables/daily.parquet")
dailyStatisticsDf.show()

// COMMAND ----------

def countMean(dataset: Dataset[Answer], predicate: Answer => Boolean): Double = {
  return  dataset
      .filter(predicate)
      .select(avg("score"))
      .first.getDouble(0)
}

def countMedian(dataset: Dataset[Answer], predicate: Answer => Boolean): Double = {
 return dataset.filter(predicate).stat.approxQuantile("score", Array(0.5), 0.1)(0) 
}

def countMeanAndMedianOfAnswerScores(dataset: Dataset[Answer], predicate: Answer => Boolean): (Double, Double) = {
 return (countMean(dataset, predicate), countMedian(dataset, predicate))
}

// COMMAND ----------

// MAGIC %md
// MAGIC What is the average and median score of the answers containing references (links) vs. those which don't have any refs(links)?

// COMMAND ----------

def extractLinkBody(html: String): Elements = {
  Jsoup.parse(html).select("a[href]")
}

// COMMAND ----------

val containsLink = (a: Answer) => !extractLinkBody(a.body).isEmpty
println(countMeanAndMedianOfAnswerScores(answersDs,containsLink(_)))
println(countMeanAndMedianOfAnswerScores(answersDs, !containsLink(_)))

// COMMAND ----------

// MAGIC %md
// MAGIC What is the average and median score of the answers containing code snippets against those who don't have any code snippets?

// COMMAND ----------

def extractCodeBody(html: String): Elements = {
    Jsoup.parse(html).select("pre").select("code")
}

// COMMAND ----------

val containsCode = (a: Answer) => !extractCodeBody(a.body).isEmpty
println(countMeanAndMedianOfAnswerScores(answersDs,containsCode(_)))
println(countMeanAndMedianOfAnswerScores(answersDs, !containsCode(_)))

// COMMAND ----------

// MAGIC %md
// MAGIC Code length (lines) distribution (visualize)

// COMMAND ----------

// MAGIC %md
// MAGIC Распределение по графику похоже на геометрическое, в котором p --- вероятность, того, что user закончит вводить код(включая "не начинание ввода").

// COMMAND ----------

val codeLengthProvider: (String => Int) = extractCodeBody(_).html.split("\\r\\n|\\r|\\n").length

// COMMAND ----------

val codeLenDf = answersDs.select(col("body"))
  .unionAll(questionsDs.select(col("body")))
  .map(r => codeLengthProvider(r(0).toString))

display(codeLenDf)


// COMMAND ----------

// MAGIC %md
// MAGIC Crrelation between the code length and answer score (visualize)

// COMMAND ----------

val codeLenUdf = udf(codeLengthProvider)
val codeLenScoreDf = answersDs
    .withColumn("len", codeLenUdf($"body"))
    .select("score", "len")
    
display(codeLenScoreDf)  

// COMMAND ----------

// MAGIC %md
// MAGIC Statistics on the major topics (top 100 tags de-duped), such as django, flask, numpy, pandas etc.

// COMMAND ----------

val majorTopics = tagsDs
      .groupBy("tag")
      .agg(count("id").alias("amount"))
      .orderBy(desc("amount"))
      .limit(100)
      .cache

majorTopics.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Experts (users with the best score) per major topics 

// COMMAND ----------

val totalTopicScoreDf = majorTopics
      .join(tagsDs, Seq("tag"), "left_outer")
      .join(questionsDs, Seq("id"), "inner")
      .groupBy("tag", "ownerUserId")
      .agg(sum("score").alias("total_score"))

val overTag = Window.partitionBy($"tag").orderBy($"total_score".desc)
val ranked = totalTopicScoreDf.withColumn("rank", dense_rank.over(overTag))
ranked
    .select("tag", "total_score", "ownerUserId")
    .where($"rank" <= 1)
    .orderBy(desc("total_score"))
    .show()

// COMMAND ----------

// MAGIC %md
// MAGIC What are the most popular (citated) resources referenced in the answers (top 100)

// COMMAND ----------

val extractLinkUdf = udf((b: String) => extractLinkBody(b).html)
answersDs
  .filter(containsLink)
  .na.drop("all")
  .withColumn("link", extractLinkUdf($"body"))
  .withColumn("host", callUDF("parse_url", $"link", lit("HOST")))
  .select("host")
  .na.drop("all")
  .groupBy("host")
  .agg(count("*").alias("amount_of_references"))
  .orderBy(desc("amount_of_references"))
  .limit(100)
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Seasonality for both questions and answers (yearly)

// COMMAND ----------

val selectCols = Seq($"score", month($"creationDate").alias("month"))
val seasonalityDf = questionsDs
      .select(selectCols: _*)
      .union(answersDs.select(selectCols: _*))
      .groupBy("month")
      .agg(mean("score"))
      .orderBy("month")

display(seasonalityDf)

// COMMAND ----------


