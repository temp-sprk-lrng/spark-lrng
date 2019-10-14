package com.example.etl.defenition

import org.apache.spark.sql.{DataFrame, Dataset}

case class EtlTriDefinition[T, U, V](sourceDF1: Dataset[T],
                                     sourceDF2: Dataset[U],
                                     sourceDF3: Dataset[V],
                                     transformer: (Dataset[T], Dataset[U], Dataset[V]) => DataFrame,
                                     write: (DataFrame => Unit)) extends Process {

  def process(): Unit = {
    write(transformer(sourceDF1, sourceDF2, sourceDF3))
  }
}
