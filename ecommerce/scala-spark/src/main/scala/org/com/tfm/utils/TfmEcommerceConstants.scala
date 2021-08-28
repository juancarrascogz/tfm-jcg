package org.com.tfm.utils

trait TfmEcommerceConstants {

  val PROJECT_ID: String = "proyecto-ucm-315417"

  val CHECKPOINT_PATH: String = "gs://tfm-jcg/checkpoint"

  val WINDOW_LENGTH: String = "0"
  val SLIDING_INTERVAL: String = "30"
  val TOTAL_RUNNING_TIME: String = "1"

  val DATASET_NAME: String = "TfmEcommerce"
  val COMPLETE_DATASET_TABLE_NAME: String = "completeDataset"

  val ORDERS_TABLE: String = "orders"
  val PRODUCTS_TABLE: String = "products"
  val CUSTOMERS_TABLE: String = "customers"
  val GEOLOCATION_TABLE: String = "geolocation"
  val ITEMS_TABLE: String = "items"
  val PAYMENTS_TABLE: String = "payments"
  val REVIEWS_TABLE: String = "reviews"
  val SELLERS_TABLE: String = "sellers"
}
