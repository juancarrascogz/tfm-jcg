package org.com.tfm

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.com.tfm.streamprocess.{TfmEcommerceCustomersProcessing, TfmEcommerceGeolocationProcessing, TfmEcommerceItemProcessing, TfmEcommerceOrdersProcessing, TfmEcommerceProductsProcessing, TfmEcommerceReviewsProcessing}
import org.com.tfm.utils.TfmEcommerceConstants

object TfmEcommerceMain extends TfmEcommerceConstants{

  def main(args: Array[String]): Unit = {

    implicit val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.driver.allowMultipleContext", "true")
      .getOrCreate()

    val Seq(datasetToRead) = args.toSeq

    val ssc = StreamingContext.getOrCreate(CHECKPOINT_PATH,
      () => createContext(SLIDING_INTERVAL, CHECKPOINT_PATH))

    val (topicName, subscriptionName) = (datasetToRead, s"subs-${datasetToRead}")

    val streamingContext: StreamingContext = datasetToRead match{
      case "orders" => TfmEcommerceOrdersProcessing.ordersMain(topicName, subscriptionName, ssc)
      case "products" => TfmEcommerceProductsProcessing.productsMain(topicName, subscriptionName, ssc)
      case "customers" => TfmEcommerceCustomersProcessing.customersMain(topicName, subscriptionName, ssc)
      case "geolocation" => TfmEcommerceGeolocationProcessing.geolocationMain(topicName, subscriptionName, ssc)
      case "order-items" => TfmEcommerceItemProcessing.itemMain(topicName, subscriptionName, ssc)
      case "order-reviews" => TfmEcommerceReviewsProcessing.reviewsMain(topicName, subscriptionName, ssc)
      case "payments" => TfmEcommerceReviewsProcessing.reviewsMain(topicName, subscriptionName, ssc)
      case "sellers" => TfmEcommerceReviewsProcessing.reviewsMain(topicName, subscriptionName, ssc)
      case "_" => throw new Exception(s"Invalid argument $datasetToRead. Please, set a valid one.")
    }

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(1000 * 60 * TOTAL_RUNNING_TIME.toInt)
    streamingContext.stop()

  }

  def createContext(
    slidingInterval: String,
    checkpointDirectory: String
  )(implicit sparkSession: SparkSession): StreamingContext = {

    val sparkContext: SparkContext = sparkSession.sparkContext
    sparkContext.getConf.set("spark.driver.allowMultipleContexts", "true")
    sparkContext.getConf.set("spark.scheduler.mode", "FAIR")

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(slidingInterval.toInt))

    ssc.checkpoint(checkpointDirectory)

    ssc
  }

}
