package com.example.etl.common

import org.apache.spark.sql.{DataFrame, Dataset}

trait TriTransformer[T, U, V] {
  def transform(ds1: Dataset[T], ds2: Dataset[U], ds3: Dataset[V]): DataFrame
}
