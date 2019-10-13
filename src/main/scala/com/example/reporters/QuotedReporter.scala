package com.example.reporters

import java.net.{URI, URISyntaxException}

import com.example.model.Answer
import com.example.reporters.common.{ReportUnit, Reporter}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.jsoup.Jsoup

case class QuotedReporter(answersDs: Dataset[Answer], limit: Int) extends Reporter {

  import com.example.session.SparkSessionHolder.spark.implicits._;

  override val reportData: ReportUnit = {
    val topQuoteDf = answersDs
      .filter(a => !Jsoup.parse(a.body).select("a[href]").isEmpty)
      .map(getHost)
      .na
      .drop("all")
      .select(col("value").alias("host"))
      .groupBy("host")
      .agg(count("*").alias("amount_of_references"))
      .orderBy(desc("amount_of_references"))
      .limit(limit)

    common.ReportUnit(topQuoteDf, "top_quote")
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
