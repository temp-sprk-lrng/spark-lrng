package com.example.etl.common

import org.apache.spark.sql.{DataFrame, Dataset}

trait Transformer[T] {
  def transform(ds: Dataset[T]): DataFrame
}
