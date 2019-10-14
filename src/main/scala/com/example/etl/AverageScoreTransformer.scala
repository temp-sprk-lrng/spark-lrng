package com.example.etl

import com.example.etl.common.Transformer
import com.example.etl.common.util.AverageUtil
import org.apache.spark.sql.{DataFrame, Dataset}

case class AverageScoreTransformer[T](predicate: T => Boolean) extends Transformer[T] {
  override def transform(ds: Dataset[T]): DataFrame = {
    val result = AverageUtil.countMeanAndMedian(ds, predicate, "score").toString
    import com.example.session.SparkSessionHolder.spark.implicits._;
    Seq(result).toDF("avg")
  }
}
