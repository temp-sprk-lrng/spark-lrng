package com.example.reporters

import java.io
import java.io.{File, PrintWriter}

trait SimpleLogReporter extends Reporter {
  val reportData: Seq[ReportUnit]

  override def report(): Unit = {
    reportData.foreach(u => {
      val filename = "/home/sei/data/out/" + u.name
      new File(filename).delete
      new io.PrintWriter(filename) {
        write(u.result); close
      }
    })
  }
}
