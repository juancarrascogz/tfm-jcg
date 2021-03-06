package org.com.tfm.streamprocess

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.com.tfm.utils.TfmEcommerceConstants
import org.com.tfm.utils.TfmEcommerceUtils

object TfmEcommerceOrdersProcessing extends TfmEcommerceConstants{

  case class Order(
    order_id: String,
    customer_id: String,
    order_status: String,
    order_purchase_timestamp: String,
    order_approved_at: String,
    order_delivered_carrier_date: String,
    order_delivered_customer_date: String,
    order_estimated_delivery_date: String
  )

  def ordersMain(topicName: String, subscriptionName: String, ssc: StreamingContext)
  (implicit spark: SparkSession): StreamingContext = {

    import spark.implicits._

    val messageStream: DStream[String] = TfmEcommerceUtils.createStream(topicName, subscriptionName, ssc)

    val streamedOrders: DStream[Order] = messageStream
      .window(Seconds(WINDOW_LENGTH.toInt), Seconds(SLIDING_INTERVAL.toInt))
      .transform(extractOrders(_))

    streamedOrders.foreachRDD(
      rdd => {
        rdd.toDF().transform(TfmEcommerceUtils.writeToBigquery(DATASET_NAME, ORDERS_TABLE))
        rdd.toDF().show(false)
        logger.info(s"Stream processed with ${rdd.toDF().count} values")
      }
    )

    ssc
  }

  def extractOrders(input: RDD[String]): RDD[Order] = {
    input
      .map(el => el.substring(1, el.length() - 1).split(","))
      .map(item => Order(
        item(0),
        item(1),
        item(2),
        item(3),
        item(4),
        item(5),
        item(6),
        item(7)
      ))
  }
}
