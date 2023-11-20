from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.models.baseoperator import chain


class DataprocSparkJob:
    def __init__(
            self,
            dag_id,
            gcp_project_id,
            pyspark_uri,
            py_files_uris=None,
            cluster_name='spark-cluster',
            cluster_region='us-central1',
            num_workers=2,
            machine_type='n1-standard-2',
            default_args=None,
    ):
        self.dag_id = dag_id
        self.gcp_project_id = gcp_project_id
        self.pyspark_uri = pyspark_uri
        self.py_files_uris = py_files_uris or []
        self.cluster_name = cluster_name
        self.cluster_region = cluster_region
        self.num_workers = num_workers
        self.machine_type = machine_type

        self.default_args = default_args or {
            'owner': 'airflow',
            'start_date': days_ago(1),
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }

        # Building DAG
        self.dag = DAG(
            dag_id,
            default_args=self.default_args,
            description=f'Create Dataproc cluster, run PySpark job {dag_id}, and delete cluster',
            schedule_interval=None,
            catchup=False
        )

        # Orchestrate workflow
        self.create_cluster_operator = self._create_cluster_operator()
        self.run_pyspark_job_operator = self._run_pyspark_job_operator()
        self.delete_cluster_operator = self._delete_cluster_operator()

        self._set_dependencies()

        def _create_cluster_operator(self):
            """
            Create a spark cluster based on inserted configuration.
            Cluster will install all necessary dependencies
            for pyspark to run a job.
            """
            return DataprocCreateClusterOperator(
                task_id='create_cluster',
                project_id=self.gcp_project_id,
                cluster_config={
                    'master_config': {
                        'num_instances': 1,
                        'machine_type_uri': self.machine_type
                    },
                    'worker_config': {
                        'num_instances': self.num_workers,
                        'machine_type': self.machine_type
                    }
                },
                region=self.cluster_region,
                cluster_name=self.cluster_name,
                dag=self.dag,
            )

        def _run_pyspark_job_operator(self):
            """
            Run an actual pyspark script inside created cluster.
            You will require main.py script 
            to run it and zipper .py dependencies.

            Specify an UR from bucker to make it working.
            """
            return DataProcPySparkOperator(
                task_id='run_pyspark_job',
                main=self.pyspark_uri,
                py_files=self.py_files_uris,
                cluster_name=self.cluster_name,
                region=self.cluster_region,
                dag=self.dag
            )

        def _delete_cluster_operator(self):
            """
            After successfull job run delete cluster
            to save costs and resources.
            """
            return DataprocDeleteClusterOperator(
                task_id='delete_cluster',
                project_id=self.gcp_project_id,
                cluster_name=self.cluster_name,
                region=self.cluster_region,
                trigger_rule='all_done',
                dag=self.dag
            )

        def _set_dependencies(self):
            """
            Create a chain for airflow to work
            """
            chain(
                self.create_cluster_operator,
                self.run_pyspark_job_operator,
                self.delete_cluster_operator
            )

        def get_dag(self):
            return self.dag
