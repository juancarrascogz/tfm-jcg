package org.com.tfm.functional

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.com.tfm.utils.{TfmEcommerceConstants, TfmEcommerceUtils}

object TfmEcommerceFunctional extends TfmEcommerceConstants{

  def tfmEcommerceFunctionalMain(implicit spark: SparkSession): Unit = {

    logger.info("Functional Process Start")

    TfmEcommerceUtils
      .readFromBigquery(DATASET_NAME, CUSTOMERS_TABLE)
      .transform(leftOuterJoinEcommerce(TfmEcommerceUtils.readFromBigquery(DATASET_NAME, ORDERS_TABLE), "customer_id"))
      .transform(leftOuterJoinEcommerce(TfmEcommerceUtils.readFromBigquery(DATASET_NAME, ITEMS_TABLE), "order_id"))
      .transform(leftOuterJoinEcommerce(TfmEcommerceUtils.readFromBigquery(DATASET_NAME, PRODUCTS_TABLE), "product_id"))
      .transform(leftOuterJoinEcommerce(TfmEcommerceUtils.readFromBigquery(DATASET_NAME, REVIEWS_TABLE), "order_id"))
      .transform(leftOuterJoinEcommerce(TfmEcommerceUtils.readFromBigquery(DATASET_NAME, PAYMENTS_TABLE), "order_id"))
      .transform(leftOuterJoinEcommerce(TfmEcommerceUtils.readFromBigquery(DATASET_NAME, SELLERS_TABLE), "seller_id"))
      .withColumnRenamed("seller_zip_code_prefix", "geolocation_zip_code_prefix")
      .withColumnRenamed("seller_city", "geolocation_city")
      .withColumnRenamed("seller_state", "geolocation_state")
      .transform(leftOuterJoinEcommerce(TfmEcommerceUtils.readFromBigquery(DATASET_NAME, GEOLOCATION_TABLE), "geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"))
      .transform(TfmEcommerceUtils.writeToBigquery(DATASET_NAME, COMPLETE_DATASET_TABLE_NAME))

      logger.info("Functional Process Ended")
  }

  def leftOuterJoinEcommerce(rightDf: DataFrame, joinKey: String*)(leftDf: DataFrame): DataFrame =
    leftDf.join(
      rightDf,
      joinKey,
      "left_outer"
    )

}