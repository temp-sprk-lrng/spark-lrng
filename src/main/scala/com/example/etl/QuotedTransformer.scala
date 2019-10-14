package com.example.etl

import java.net.{URI, URISyntaxException}

import com.example.model.Answer
import com.example.etl.common.Transformer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.jsoup.Jsoup

case class QuotedTransformer(limit: Int) extends Transformer[Answer] {

  import com.example.session.SparkSessionHolder.spark.implicits._;


  override def transform(answersDs: Dataset[Answer]): DataFrame = {
    answersDs
      .filter(a => !Jsoup.parse(a.body).select("a[href]").isEmpty)
      .map(getHost)
      .na
      .drop("all")
      .select(col("value").alias("host"))
      .groupBy("host")
      .agg(count("*").alias("amount_of_references"))
      .orderBy(desc("amount_of_references"))
      .limit(limit)
  }

  private def getHost(a: Answer) = {
    try {
      val url = Jsoup.parse(a.body).select("a[href]").html
      new URI(url).getHost
    } catch {
      case _: URISyntaxException => null
    }
  }
}
