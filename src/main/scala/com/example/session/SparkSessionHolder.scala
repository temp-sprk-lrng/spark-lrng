package com.example.session

import org.apache.spark.sql.SparkSession

object SparkSessionHolder {
  val spark = SparkSession.builder().appName("GitHub push counter").master("local[*]").getOrCreate()
}
