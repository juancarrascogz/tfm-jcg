package org.com.tfm.streamprocess

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.com.tfm.utils.{TfmEcommerceConstants, TfmEcommerceUtils}

object TfmEcommercePaymentsProcessing extends TfmEcommerceConstants{

  def paymentsMain(topicName: String, subscriptionName: String, ssc: StreamingContext)
  (implicit spark: SparkSession): StreamingContext = {

    import spark.implicits._

    val messageStream: DStream[String] = TfmEcommerceUtils.createStream(topicName, subscriptionName, ssc)

    val streamedPayment: DStream[Payment] = messageStream
      .window(Seconds(WINDOW_LENGTH.toInt), Seconds(SLIDING_INTERVAL.toInt))
      .transform(extractPayments(_))

    streamedPayment.foreachRDD(
      rdd => rdd.toDF().transform(TfmEcommerceUtils.writeToBigquery(DATASET_NAME, PAYMENTS_TABLE))
    )

    ssc

  }

  def extractPayments(input: RDD[String]): RDD[Payment] = {
    input
      .map(el => el.substring(1, el.length() - 1).split(","))
      .map(item => Payment(
        item(0),
        item(1),
        item(2),
        item(3),
        item(4)
      ))
  }
}
