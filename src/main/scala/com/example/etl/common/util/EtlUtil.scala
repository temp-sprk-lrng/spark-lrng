package com.example.etl.common.util

import com.example.session.SparkSessionHolder
import com.example.util.CommonUtil
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode}

import scala.reflect.runtime.universe

object EtlUtil {
  def readCsv[T <: Product : universe.TypeTag](filename: String): Dataset[T] = {
    import SparkSessionHolder.spark.implicits._

    SparkSessionHolder.spark.read
      .option("multiLine", true)
      .option("header", true)
      .schema(Encoders.product[T].schema)
      .option("escape", "\"")
      .csv(CommonUtil.baseFileLocation + "/data/" + filename)
      .na.drop("any")
      .as[T]
      .limit(100)
      .cache
  }

  def writeToS3(df: DataFrame, filename: String) = {
    df.write.mode(SaveMode.Overwrite).parquet(CommonUtil.baseFileLocation + "/out/" + filename)
  }
}
