// Databricks notebook source
import java.sql.Timestamp
import java.net.{URI, URISyntaxException}


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.types.DateType
import org.jsoup.Jsoup


import spark.implicits._ //!!!


// COMMAND ----------

case class Question(id: Long,
                    ownerUserId: Long,
                    creationDate: Timestamp,
                    score: Long,
                    title: String,
                    body: String)

val questionsDs = spark.read
      .option("multiLine", true)
      .option("header", true)
      .schema(Encoders.product[Question].schema)
      .option("escape", "\"")
      .csv("/FileStore/tables/Questions.csv")
      .na.drop("all")
      .as[Question]
      
questionsDs.cache()


// COMMAND ----------

case class Answer(id: Long,
                  ownerUserId: Long,
                  creationDate: Timestamp,
                  partnerId: Long,
                  score: Long,
                  body: String)

val answersDs = spark.read
      .option("multiLine", true)
      .option("header", true)
      .schema(Encoders.product[Answer].schema)
      .option("escape", "\"")
      .csv("/FileStore/tables/Answers.csv")
      .na.drop("all")
      .as[Answer]

answersDs.cache()

// COMMAND ----------

case class Tag(id: Long,
               tag: String)

val tagsDs = spark.read
      .option("multiLine", true)
      .option("header", true)
      .schema(Encoders.product[Tag].schema)
      .option("escape", "\"")
      .csv("/FileStore/tables/Tags.csv")
      .na.drop("all")
      .as[Tag]

tagsDs.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC Collect daily statistics (number of unique users posted, number of questions and answers) into a separate table and export to a Parquet file.

// COMMAND ----------

val questionNaFreeDs = questionsDs
      .withColumn("creationDate", col("creationDate").cast(DateType))
      .na.drop(Seq("creationDate"))
val answersNaFreeDs = answersDs
  .withColumn("creationDate", col("creationDate").cast(DateType))
  .na.drop(Seq("creationDate"))

val questionCols = Seq(
  lit(null).alias("answerId"),
  col("ownerUserId"),
  col("creationDate").cast(DateType),
  col("id").alias("questionId")
)
val answerCols = Seq(
  col("id").alias("answerId"),
  col("ownerUserId"),
  col("creationDate").cast(DateType),
  lit(null).alias("questionId")
)
val unionDf = questionNaFreeDs.select(questionCols: _*).unionAll(answersNaFreeDs.select(answerCols: _ *))
val dailyStatisticsDf = unionDf
  .groupBy("creationDate")
  .agg(
  countDistinct("ownerUserId").alias("unique_users"),
  count("questionId").alias("number_of_questions"),
  count("answerId").alias("number_of_answers")
)

dbutils.fs.rm("/FileStore/tables/daily.parquet", true) 
dailyStatisticsDf.write.parquet("/FileStore/tables/daily.parquet")
dailyStatisticsDf.show()

// COMMAND ----------

def countMeanAndMedianOfAnswerScores(dataset: Dataset[Answer], predicate: Answer => Boolean): (Double, Double) = {
    val mean = dataset
      .filter(predicate)
      .select(avg("score"))
      .take(1)(0)(0).toString().toDouble

    val median = dataset.filter(predicate).stat.approxQuantile("score", Array(0.5), 0.1)(0)

    (mean, median)
}

// COMMAND ----------

// MAGIC %md
// MAGIC What is the average and median score of the answers containing references (links) vs. those which don't have any refs(links)?

// COMMAND ----------

val containsLink = (a: Answer) => !Jsoup.parse(a.body).select("a[href]").isEmpty
println(countMeanAndMedianOfAnswerScores(answersDs,containsLink(_)))
println(countMeanAndMedianOfAnswerScores(answersDs, !containsLink(_)))

// COMMAND ----------

// MAGIC %md
// MAGIC What is the average and median score of the answers containing code snippets against those who don't have any code snippets?

// COMMAND ----------

val containsLink = (a: Answer) => !Jsoup.parse(a.body).select("code").isEmpty
println(countMeanAndMedianOfAnswerScores(answersDs,containsLink(_)))
println(countMeanAndMedianOfAnswerScores(answersDs, !containsLink(_)))

// COMMAND ----------

// MAGIC %md
// MAGIC Code length (lines) distribution (visualize)

// COMMAND ----------

// MAGIC %md
// MAGIC Распределение по графику похоже на геометрическое, в котором p --- вероятность, того, что user закончит вводить код(включая "не начинание ввода"). Проверка гипотезы будет добавлена 

// COMMAND ----------

val codeLengthProvider: (String => Int) = Jsoup.parse(_).select("code").html.length
val codeLenDf = answersDs.select(col("body"))
  .unionAll(questionsDs.select(col("body")))
  .map(r => codeLengthProvider(r(0).toString))

display(codeLenDf)


// COMMAND ----------

// MAGIC %md
// MAGIC Crrelation between the code length and answer score (visualize)

// COMMAND ----------

val codeLenScoreDf = answersDs
    .map(a => (a.score, Jsoup.parse(a.body).select("code").html.length))
    .select(col("_1").alias("score"), col("_2").alias("length"))

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

totalTopicScoreDf
  .groupBy("tag")
  .agg(max("total_score").alias("total_score"))
  .join(totalTopicScoreDf, Seq("tag", "total_score"), "inner")
  .orderBy(desc("total_score"))
  .limit(100)
  .show()


// COMMAND ----------

// MAGIC %md
// MAGIC What are the most popular (citated) resources referenced in the answers (top 100)

// COMMAND ----------

answersDs
   .filter(a => !Jsoup.parse(a.body).select("a[href]").isEmpty)
   .map(a => {
   try {
     val url = Jsoup.parse(a.body).select("a[href]").html
     new URI(url).getHost
     }  catch {
       case ex: URISyntaxException => null
     }
   })
   .na
   .drop("all")
   .select(col("value").alias("host"))
   .groupBy("host")
   .agg(count("*").alias("amount_of_references"))
   .orderBy(desc("amount_of_references"))
   .limit(100)
   .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Seasonality for both questions and answers (yearly)

// COMMAND ----------

val selectCols = Seq($"id", year($"creationDate").alias("year"))
questionsDs
  .select(selectCols: _*)
  .union(answersDs.select(selectCols: _*))
  .groupBy("year")
  .agg(count("*").alias("amount_of_questions_answers"))
  .show()

// COMMAND ----------


