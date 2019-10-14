package com.example.etl.defenition

import org.apache.spark.sql.{DataFrame, Dataset}

case class EtlBiDefinition[U, T](sourceDf1: Dataset[U],
                                 sourceDf2: Dataset[T],
                                 transformer: (Dataset[U], Dataset[T]) => DataFrame,
                                 write: (DataFrame => Unit)) extends Process {

  def process(): Unit = {
    write(transformer(sourceDf1, sourceDf2))
  }
}
