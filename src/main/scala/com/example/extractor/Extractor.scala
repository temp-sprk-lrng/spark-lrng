package com.example.extractor

import com.example.session.SparkSessionHolder
import org.apache.spark.sql.{Dataset, Encoders}

import scala.reflect.runtime.universe

object Extractor {

  def readCsv[T <: Product : universe.TypeTag](filename: String): Dataset[T] = {
    import SparkSessionHolder.spark.implicits._

    SparkSessionHolder.spark.read
      .option("multiLine", true)
      .option("header", true)
      .schema(Encoders.product[T].schema)
      .option("escape", "\"")
      .csv(filename)
      .na.drop("any")
      .limit(1000)
      .as[T]
      .cache
  }
}


