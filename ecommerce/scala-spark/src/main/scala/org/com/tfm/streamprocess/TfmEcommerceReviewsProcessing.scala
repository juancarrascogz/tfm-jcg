package org.com.tfm.streamprocess

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.com.tfm.utils.{TfmEcommerceConstants, TfmEcommerceUtils}

object TfmEcommerceReviewsProcessing extends TfmEcommerceConstants{

  case class Review(
    review_id: String,
    order_id: String,
    review_score: String,
    review_comment_title: String,
    review_comment_message: String,
    review_creation_date: String,
    review_answer_timestamp: String)

  def reviewsMain(topicName: String, subscriptionName: String, ssc: StreamingContext)
  (implicit spark: SparkSession): StreamingContext = {

    import spark.implicits._

    val messageStream: DStream[String] = TfmEcommerceUtils.createStream(topicName, subscriptionName, ssc)

    val streamedReview: DStream[Review] = messageStream
      .window(Seconds(WINDOW_LENGTH.toInt), Seconds(SLIDING_INTERVAL.toInt))
      .transform(extractReviews(_))

    streamedReview.foreachRDD(
      rdd => {
        rdd.toDF().transform(TfmEcommerceUtils.writeToBigquery(DATASET_NAME, REVIEWS_TABLE))
        rdd.toDF().show(false)
      }
    )

    ssc

  }

  def extractReviews(input: RDD[String]): RDD[Review] = {
    input
      .map(el => el.substring(1, el.length() - 1).split(","))
      .map(item => Review(
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
