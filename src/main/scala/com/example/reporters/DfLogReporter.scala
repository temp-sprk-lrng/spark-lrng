package com.example.reporters

import org.apache.spark.sql.SaveMode

trait DfLogReporter extends Reporter {
  val reportData: Seq[ReportDfUnit]

  override def report(): Unit = {
    reportData.foreach(u => u.df.write.mode(SaveMode.Overwrite).parquet("/home/sei/data/out/" + u.name))
  }
}
