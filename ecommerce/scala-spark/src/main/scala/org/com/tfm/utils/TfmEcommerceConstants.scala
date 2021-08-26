package org.com.tfm.utils

trait TfmEcommerceConstants {

  val PROJECT_ID: String = "proyecto-ucm-315417"

  val CHECKPOINT_PATH: String = "gs://tfm-jcg/checkpoint"

  val WINDOW_LENGTH: String = "0"
  val SLIDING_INTERVAL: String = "30"
  val TOTAL_RUNNING_TIME: String = "5"

  val DATASET_NAME: String = "TfmEcommerce"

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
  val ORDERS_TABLE: String = "orders"

  case class Product(
    product_id: String,
    product_category_name: String,
    product_name_lenght: String,
    product_description_lenght: String,
    product_photos_qty: String,
    product_weight_g: String,
    product_length_cm: String,
    product_height_cm: String,
    product_width_cm: String
  )
  val PRODUCTS_TABLE: String = "products"

  case class Customer(
    customer_id: String,
    customer_unique_id: String,
    customer_zip_code_prefix: String,
    customer_city: String,
    customer_state: String
  )
  val CUSTOMERS_TABLE: String = "customers"

  case class Geolocation(
    geolocation_zip_code_prefix: String,
    geolocation_lat: String,
    geolocation_lng: String,
    geolocation_city: String,
    geolocation_state: String
  )
  val GEOLOCATION_TABLE: String = "geolocation"

  case class Item(
    order_id: String,
    order_item_id: String,
    product_id: String,
    seller_id: String,
    shipping_limit_date: String,
    price: String,
    freight_value: String
  )
  val ITEMS_TABLE: String = "items"

  case class Payment(
    order_id: String,
    payment_sequential: String,
    payment_type: String,
    payment_installments: String,
    payment_value: String
  )
  val PAYMENTS_TABLE: String = "payments"

  case class Review(
    review_id: String,
    order_id: String,
    review_score: String,
    review_comment_title: String,
    review_comment_message: String,
    review_creation_date: String,
    review_answer_timestamp: String
  )
  val REVIEWS_TABLE: String = "reviews"

  case class Seller(
    review_id: String,
    order_id: String,
    review_score: String,
    review_comment_title: String,
    review_comment_message: String,
    review_creation_date: String,
    review_answer_timestamp: String
  )
  val SELLERS_TABLE: String = "sellers"
}
