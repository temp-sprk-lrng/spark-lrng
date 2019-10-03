package com.example.reporters

import com.example.model.Tag
import com.example.reporters.common.{DfLogReporter, ReportDfUnit}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{count, desc}

case class MajorTopicsReporter(tagsDs: Dataset[Tag]) extends DfLogReporter {

  override val reportData: ReportDfUnit = {
    val majorTopics = tagsDs
      .groupBy("tag")
      .agg(count("id").alias("amount"))
      .orderBy(desc("amount"))
      .limit(100)
      .cache

    common.ReportDfUnit(majorTopics, "major_topics")
  }
}
