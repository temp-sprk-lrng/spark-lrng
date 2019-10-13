package com.example.session

import org.apache.spark.sql.SparkSession

object SparkSessionHolder {
  val spark = SparkSession.builder().appName("spark-lrng").master("local[*]").getOrCreate()

  def init(awsAccessKeyId: String, awsSecretAccessKey: String) {
    SparkSessionHolder.spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    SparkSessionHolder.spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", awsAccessKeyId)
    SparkSessionHolder.spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", awsSecretAccessKey)
  }
}
