package org.com.tfm.streamprocess

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.com.tfm.utils.{TfmEcommerceConstants, TfmEcommerceUtils}

object TfmEcommerceItemProcessing extends TfmEcommerceConstants{

  def itemMain(topicName: String, subscriptionName: String, ssc: StreamingContext)
  (implicit spark: SparkSession): StreamingContext = {

    import spark.implicits._

    val messageStream: DStream[String] = TfmEcommerceUtils.createStream(topicName, subscriptionName, ssc)

    val streamedItem: DStream[Item] = messageStream
      .window(Seconds(WINDOW_LENGTH.toInt), Seconds(SLIDING_INTERVAL.toInt))
      .transform(extractItems(_))

    streamedItem.foreachRDD(
      rdd => rdd.toDF().transform(TfmEcommerceUtils.writeToBigquery(DATASET_NAME, ITEMS_TABLE))
    )

    ssc

  }

  def extractItems(input: RDD[String]): RDD[Item] = {
    input
      .map(el => el.substring(1, el.length() - 1).split(","))
      .map(item => Item(
        item(0),
        item(1),
        item(2),
        item(3),
        item(4),
        item(5),
        item(6)
      ))
  }
}
