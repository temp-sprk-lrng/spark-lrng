package com.example.reporters.common

import org.apache.spark.sql.SaveMode

trait Reporter {
  val reportData: ReportUnit

  def report(): Unit = {
    reportData.df.write.mode(SaveMode.Overwrite).parquet("s3a://spark-lrng-d.sei/out/" + reportData.name)
  }
}
