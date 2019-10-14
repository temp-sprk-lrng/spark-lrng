package com.example.reporters

import com.example.model.Tag
import com.example.reporters.common.{ReportUnit, Reporter}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{count, desc}

case class MajorTopicsReporter(tagsDs: Dataset[Tag], limit: Int) extends Reporter {

  override val reportData: ReportUnit = {
    val majorTopics = tagsDs
      .groupBy("tag")
      .agg(count("id").alias("amount"))
      .orderBy(desc("amount"))
      .limit(limit)
      .cache

    common.ReportUnit(majorTopics, "major_topics")
  }
}
