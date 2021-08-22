import csv

# """Publishes multiple messages to a Pub/Sub topic."""
def publish_messages(project_id, topic_id, dataset):
    # [START pubsub_publish]
    from google.cloud import pubsub_v1
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    with open(dataset) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are: {", ".join(row)}')
                line_count += 1
            else:
                data = f"{row}"
                # Data must be a bytestring
                data = data.encode("utf-8")
                # When you publish a message, the client returns a future.
                future = publisher.publish(topic_path, data)
                print(future.result())

                line_count += 1
        print(f'Processed {line_count} lines.')

    print(f"Published messages to {topic_path}.\n")
    # [END pubsub_publish]

def main():    
    publish_messages("proyecto-ucm-315417", "customers", "../dataset/olist_customers_dataset.csv")
    publish_messages("proyecto-ucm-315417", "geolocation", "../dataset/olist_geolocation_dataset.csv")
    publish_messages("proyecto-ucm-315417", "order-items", "../dataset/olist_order_items_dataset.csv")
    publish_messages("proyecto-ucm-315417", "payments", "../dataset/olist_order_payments_dataset.csv")
    publish_messages("proyecto-ucm-315417", "order-reviews", "../dataset/olist_order_reviews_dataset.csv")
    publish_messages("proyecto-ucm-315417", "orders", "../dataset/olist_orders_dataset.csv")
    publish_messages("proyecto-ucm-315417", "products", "../dataset/olist_products_dataset.csv")
    publish_messages("proyecto-ucm-315417", "sellers", "../dataset/olist_sellers_dataset.csv")
    publish_messages("proyecto-ucm-315417", "product-category-name-translation", "../dataset/product_category_name_translation.csv")

if __name__ == "__main__":
    main()