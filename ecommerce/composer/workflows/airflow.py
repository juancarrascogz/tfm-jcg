from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

launchClusterScript = 'gsutil -m cp gs://tfm-jcg/scripts/createCluster.sh /home/airflow/gcs/dags &&' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/createCluster.sh'

removeClusterScript = 'gsutil -m cp gs://tfm-jcg/scripts/removeCluster.sh /home/airflow/gcs/dags &&' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/removeCluster.sh'

launchtriggerDataprocJob = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags &&' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/ '

launchSparkBigQuery = 'gsutil -m cp gs://tfm-jcg/scripts/triggerDataprocJob.sh /home/airflow/gcs/dags &&' \
    'chmod -R 777 /home/airflow/gcs/dags && ' \
    '/home/airflow/gcs/dags/triggerDataprocJob.sh proyecto-ucm-315417-juandelsolete-cluster gs://tfm-jcg/jars/ '


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

launchStreamingJob = BashOperator(
    task_id='triggerDataprocJob',
    bash_command=launchtriggerDataprocJob,
    dag=dag
)

launchSparkBigQueryJob = BashOperator(
    task_id='triggerDataprocJob_2',
    bash_command=launchSparkBigQuery,
    dag=dag
)

removeCluster = BashOperator(
    task_id='RemoveCluster',
    bash_command=removeClusterScript,
    dag=dag
)

launchCluster >> launchStreamingJob >> launchSparkBigQueryJob >> removeCluster