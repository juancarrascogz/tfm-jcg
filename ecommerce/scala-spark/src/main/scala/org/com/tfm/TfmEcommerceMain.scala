package org.com.tfm

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.com.tfm.functional.TfmEcommerceFunctional
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

    datasetToRead match {
      case "functional" => TfmEcommerceFunctional.tfmEcommerceFunctionalMain
      case _ => triggerStreamProcess(datasetToRead)
    }

  }

  def createContext(
    slidingInterval: String,
    checkpointDirectory: String
  )(implicit sparkSession: SparkSession): StreamingContext = {

    val sparkContext: SparkContext = sparkSession.sparkContext
    sparkContext.getConf.set("spark.driver.allowMultipleContexts", "true")
    sparkContext.getConf.set("spark.scheduler.mode", "FAIR")

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(slidingInterval.toInt))

    ssc
  }

  def triggerStreamProcess(streamToRead: String)(implicit spark: SparkSession): Unit = {
    val fullCheckpoint = s"$CHECKPOINT_PATH/$streamToRead"
    val ssc = StreamingContext.getOrCreate(fullCheckpoint,
      () => createContext(SLIDING_INTERVAL, fullCheckpoint))

    val (topicName, subscriptionName) = (streamToRead, s"subs-${streamToRead}")

    spark.conf.set("temporaryGcsBucket",s"tfm-jcg/tmp-$streamToRead-bucket")

    val sparkStreamContext: StreamingContext = streamToRead match{
      case "orders" => TfmEcommerceOrdersProcessing.ordersMain(topicName, subscriptionName, ssc)
      case "products" => TfmEcommerceProductsProcessing.productsMain(topicName, subscriptionName, ssc)
      case "customers" => TfmEcommerceCustomersProcessing.customersMain(topicName, subscriptionName, ssc)
      case "geolocation" => TfmEcommerceGeolocationProcessing.geolocationMain(topicName, subscriptionName, ssc)
      case "order-items" => TfmEcommerceItemProcessing.itemMain(topicName, subscriptionName, ssc)
      case "order-reviews" => TfmEcommerceReviewsProcessing.reviewsMain(topicName, subscriptionName, ssc)
      case "payments" => TfmEcommerceReviewsProcessing.reviewsMain(topicName, subscriptionName, ssc)
      case "sellers" => TfmEcommerceReviewsProcessing.reviewsMain(topicName, subscriptionName, ssc)
    }

    sparkStreamContext.start()
    sparkStreamContext.awaitTerminationOrTimeout(1000 * 60 * TOTAL_RUNNING_TIME.toInt)
    sparkStreamContext.stop()
  }
}
