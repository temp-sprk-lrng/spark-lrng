package com.example.reporters.common

import org.apache.spark.sql.DataFrame

case class ReportDfUnit(df: DataFrame, name: String)
