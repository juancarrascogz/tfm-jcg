package org.com.tfm.streamprocess

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.com.tfm.utils.{TfmEcommerceConstants, TfmEcommerceUtils}

object TfmEcommerceGeolocationProcessing extends TfmEcommerceConstants{

  case class Geolocation(
    geolocation_zip_code_prefix: String,
    geolocation_lat: String,
    geolocation_lng: String,
    geolocation_city: String,
    geolocation_state: String)

  def geolocationMain(topicName: String, subscriptionName: String, ssc: StreamingContext)
  (implicit spark: SparkSession): StreamingContext = {

    import spark.implicits._

    val messageStream: DStream[String] = TfmEcommerceUtils.createStream(topicName, subscriptionName, ssc)

    val streamedGeolocation: DStream[Geolocation] = messageStream
      .window(Seconds(WINDOW_LENGTH.toInt), Seconds(SLIDING_INTERVAL.toInt))
      .transform(extractGeolocation(_))

    streamedGeolocation.foreachRDD(
      rdd => {
        rdd.toDF().transform(TfmEcommerceUtils.writeToBigquery(DATASET_NAME, GEOLOCATION_TABLE))
        rdd.toDF().show(false)
      }
    )

    ssc

  }

  def extractGeolocation(input: RDD[String]): RDD[Geolocation] = {
    input
      .map(el => el.substring(1, el.length() - 1).split(","))
      .map(item => Geolocation(
        item(0),
        item(1),
        item(2),
        item(3),
        item(4)
      ))
  }
}
