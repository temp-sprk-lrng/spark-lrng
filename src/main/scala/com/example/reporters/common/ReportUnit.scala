package com.example.reporters.common

import org.apache.spark.sql.DataFrame

case class ReportUnit(df: DataFrame, name: String)
