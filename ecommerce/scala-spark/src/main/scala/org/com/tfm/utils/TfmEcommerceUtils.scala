package org.com.tfm.utils

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}

object TfmEcommerceUtils extends TfmEcommerceConstants {

  def writeToBigquery(dataset: String, table: String)
  (inputDf: DataFrame)(implicit spark: SparkSession): DataFrame = {

    inputDf.write.format("bigquery")
      .option("table", s"$dataset.$table")
      .save()

    inputDf
  }

  def readFromBigquery(dataset: String, table: String)
  (implicit spark: SparkSession): DataFrame = {

    spark.read.format("bigquery")
      .option("table", s"$dataset.$table")
      .load()
  }

  def createStream(topicName: String, subscriptionName: String, ssc: StreamingContext): DStream[String] =
    PubsubUtils.createStream(
      ssc,
      PROJECT_ID,
      Some(topicName),
      subscriptionName,
      SparkGCPCredentials.builder.build(),
      StorageLevel.MEMORY_AND_DISK_SER_2
    ).map(el => new String(el.getData(), StandardCharsets.UTF_8))
}
