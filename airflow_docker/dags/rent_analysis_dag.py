from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from rent_analysis_functions.functions import etl_process

with DAG(
	'rent_analysis_test', 
	start_date = datetime(2023,1,1),
	schedule_interval = '0 8 * * *',
	catchup = False
	) as dag:

	task_a = PythonOperator(
			task_id = 'rent_analysis_etl',
			python_callable = etl_process,
			op_kwargs = Variable.get("rent_analysis_settings", deserialize_json=True)
		)