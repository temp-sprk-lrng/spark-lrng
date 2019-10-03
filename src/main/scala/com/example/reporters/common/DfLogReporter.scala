package com.example.reporters.common

import org.apache.spark.sql.SaveMode

trait DfLogReporter extends Reporter {
  val reportData: ReportDfUnit

  override def report(): Unit = {
    reportData.df.write.mode(SaveMode.Overwrite).parquet(System.getenv("REPORT_LOCATION") + reportData.name)
  }
}
