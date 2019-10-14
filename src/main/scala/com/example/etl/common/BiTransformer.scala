package com.example.etl.common

import org.apache.spark.sql.{DataFrame, Dataset}

;

trait BiTransformer[T, U] {
  def transform(ds1: Dataset[T], ds2: Dataset[U]): DataFrame
}
