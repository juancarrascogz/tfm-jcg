package org.com.tfm.streamprocess

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.com.tfm.utils.{TfmEcommerceConstants, TfmEcommerceUtils}

object TfmEcommerceProductsProcessing extends TfmEcommerceConstants{

  case class Product(
    product_id: String,
    product_category_name: String,
    product_name_lenght: String,
    product_description_lenght: String,
    product_photos_qty: String,
    product_weight_g: String,
    product_length_cm: String,
    product_height_cm: String,
    product_width_cm: String)

  def productsMain(topicName: String, subscriptionName: String, ssc: StreamingContext)
  (implicit spark: SparkSession): StreamingContext = {

    import spark.implicits._

    val messageStream: DStream[String] = TfmEcommerceUtils.createStream(topicName, subscriptionName, ssc)

    val streamedProducts: DStream[Product] = messageStream
      .window(Seconds(WINDOW_LENGTH.toInt), Seconds(SLIDING_INTERVAL.toInt))
      .transform(extractProducts(_))

    streamedProducts.foreachRDD(
      rdd => {
        rdd.toDF().transform(TfmEcommerceUtils.writeToBigquery(DATASET_NAME, PRODUCTS_TABLE))
        rdd.toDF().show(false)
        logger.info(s"Stream processed with ${rdd.toDF().count} values")
      }
    )

    ssc

  }

  def extractProducts(input: RDD[String]): RDD[Product] = {
    input
      .map(el => el.substring(1, el.length() - 1).split(","))
      .map(item => Product(
        item(0),
        item(1),
        item(2),
        item(3),
        item(4),
        item(5),
        item(6),
        item(7),
        item(8)
      ))
  }
}
