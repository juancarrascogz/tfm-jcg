package org.com.tfm.streamprocess

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.com.tfm.utils.{TfmEcommerceConstants, TfmEcommerceUtils}

object TfmEcommerceSellersProcessing extends TfmEcommerceConstants{

  case class Seller(
    seller_id: String,
    seller_zip_code_prefix: String,
    seller_city: String,
    seller_state: String
  )

  def sellersMain(topicName: String, subscriptionName: String, ssc: StreamingContext)
  (implicit spark: SparkSession): StreamingContext = {

    import spark.implicits._

    val messageStream: DStream[String] = TfmEcommerceUtils.createStream(topicName, subscriptionName, ssc)

    val streamedSeller: DStream[Seller] = messageStream
      .window(Seconds(WINDOW_LENGTH.toInt), Seconds(SLIDING_INTERVAL.toInt))
      .transform(extractSellers(_))

    streamedSeller.foreachRDD(
      rdd => {
        rdd.toDF().transform(TfmEcommerceUtils.writeToBigquery(DATASET_NAME, SELLERS_TABLE))
        rdd.toDF().show(false)
      }
    )

    ssc

  }

  def extractSellers(input: RDD[String]): RDD[Seller] = {
    input
      .map(el => el.substring(1, el.length() - 1).split(","))
      .map(item => Seller(
        item(0),
        item(1),
        item(2),
        item(3)
      ))
  }
}
