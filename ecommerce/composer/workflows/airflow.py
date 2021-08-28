from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


launchClusterScript = 'gsutil -m cp gs://tfm-jcg/scripts/createCluster.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/createCluster.sh '

removeClusterScript = 'gsutil -m cp gs://tfm-jcg/scripts/removeCluster.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/removeCluster.sh '

launchOrdersStreamJob = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/pubsub_spark-assembly-0.1.0-SNAPSHOT.jar orders'

launchProductsStreamJob = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/pubsub_spark-assembly-0.1.0-SNAPSHOT.jar products'

launchCustomersStreamJob = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/pubsub_spark-assembly-0.1.0-SNAPSHOT.jar customers'

launchGeolocationStreamJob = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/pubsub_spark-assembly-0.1.0-SNAPSHOT.jar geolocation'

launchItemsStreamJob = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/pubsub_spark-assembly-0.1.0-SNAPSHOT.jar order-items'

launchReviewsStreamJob = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/pubsub_spark-assembly-0.1.0-SNAPSHOT.jar order-reviews'

launchPaymentsStreamJob = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/pubsub_spark-assembly-0.1.0-SNAPSHOT.jar payments'

launchSellersStreamJob = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/pubsub_spark-assembly-0.1.0-SNAPSHOT.jar sellers'

launchFunctionalJob = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags && ' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/pubsub_spark-assembly-0.1.0-SNAPSHOT.jar functional'

default_args = {
    'owner': 'Airflow',
    'depends_on_past': True,
    'start_date': days_ago(0),
    'email': ['juandelsolete@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}
dag = DAG(
    'tfm-jcg-e-commerce-workflow',
    default_args=default_args,
    description='tfm-jcg e-commerce workflow'
)


launchCluster = BashOperator(
    task_id='LaunchCluster',
    bash_command=launchClusterScript,
    dag=dag
)

readOrders = BashOperator(
    task_id='launchOrdersStreamJob-Airflow',
    bash_command=launchOrdersStreamJob,
    dag=dag
)

readProducts = BashOperator(
    task_id='launchProductsStreamJob-Airflow',
    bash_command=launchProductsStreamJob,
    dag=dag
)

readCustomers = BashOperator(
    task_id='launchCustomersStreamJob-Airflow',
    bash_command=launchCustomersStreamJob,
    dag=dag
)

readGeolocation = BashOperator(
    task_id='launchGeolocationStreamJob-Airflow',
    bash_command=launchGeolocationStreamJob,
    dag=dag
)

readItems = BashOperator(
    task_id='launchItemsStreamJob-Airflow',
    bash_command=launchItemsStreamJob,
    dag=dag
)

readReviews = BashOperator(
    task_id='launchReviewsStreamJob-Airflow',
    bash_command=launchReviewsStreamJob,
    dag=dag
)

readPayments = BashOperator(
    task_id='launchPaymentsStreamJob-Airflow',
    bash_command=launchPaymentsStreamJob,
    dag=dag
)

readSellers = BashOperator(
    task_id='launchSellersStreamJob-Airflow',
    bash_command=launchSellersStreamJob,
    dag=dag
)

functionalLogic = BashOperator(
    task_id='launchFunctionalJob-Airflow',
    bash_command=launchFunctionalJob,
    dag=dag
)

removeCluster = BashOperator(
    task_id='RemoveCluster',
    bash_command=removeClusterScript,
    dag=dag
)



launchCluster >> [readOrders, readProducts, readCustomers, readGeolocation, readItems, readReviews, readPayments, readSellers] >> functionalLogic >> removeCluster
