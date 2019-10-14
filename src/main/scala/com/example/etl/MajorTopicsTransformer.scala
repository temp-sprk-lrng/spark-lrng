package com.example.etl

import com.example.model.Tag
import com.example.etl.common.Transformer
import org.apache.spark.sql.functions.{count, desc}
import org.apache.spark.sql.{DataFrame, Dataset}

case class MajorTopicsTransformer(limit: Int) extends Transformer[Tag] {
  override def transform(tagsDs: Dataset[Tag]): DataFrame = {
    tagsDs
      .groupBy("tag")
      .agg(count("id").alias("amount"))
      .orderBy(desc("amount"))
      .limit(limit)
  }
}
