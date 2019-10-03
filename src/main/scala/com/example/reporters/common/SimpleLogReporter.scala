package com.example.reporters.common

import java.io
import java.io.File

trait SimpleLogReporter extends Reporter {
  val reportData: ReportUnit

  override def report(): Unit = {
    val filename = System.getenv("REPORT_LOCATION") + reportData.name
    new File(filename).delete
    new io.PrintWriter(filename) {
      write(reportData.result);
      close
    }

  }
}
