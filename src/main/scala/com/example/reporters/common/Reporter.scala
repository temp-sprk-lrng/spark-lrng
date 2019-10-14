package com.example.reporters.common

import com.example.util.CommonUtil
import org.apache.spark.sql.SaveMode

trait Reporter {
  val reportData: ReportUnit

  def report(): Unit = {
    reportData.df.write.mode(SaveMode.Overwrite).parquet(CommonUtil.baseFileLocation + "/out/" + reportData.name)
  }
}
