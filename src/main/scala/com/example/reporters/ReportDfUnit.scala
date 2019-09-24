package com.example.reporters

import org.apache.spark.sql.DataFrame

case class ReportDfUnit(df: DataFrame, name: String)
