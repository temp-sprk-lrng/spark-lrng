package com.example.etl.defenition

import org.apache.spark.sql.{DataFrame, Dataset}

case class EtlDefinition[T](sourceDF: Dataset[T],
                            transform: (Dataset[T] => DataFrame),
                            write: (DataFrame => Unit)) extends Process {

  def process(): Unit = {
    write(sourceDF.transform(transform))
  }

}
