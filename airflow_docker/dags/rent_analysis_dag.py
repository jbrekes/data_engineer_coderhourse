from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from rent_analysis_functions.functions import etl_process, get_new_listings_list, send_daily_email
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

default_args = {
	'owner': 'jbrekes',
	'email': ['jbrekesdata@gmail.com'],
	'email_on_failure': True,
	'retries': 3,
	'retry_delay': timedelta(minutes=1)
}

with DAG(
	'rent_analysis', 
	start_date = datetime(2023,1,1),
	schedule_interval = '0 8 * * *',
	catchup = False
	) as dag:

	rent_analysis_etl = PythonOperator(
			task_id = 'rent_analysis_etl',
			python_callable = etl_process,
			op_kwargs = Variable.get("secret_key_rent_analysis_settings", deserialize_json=True)
		)

	get_new_listings = PythonOperator(
			task_id = 'get_new_listings',
			python_callable = get_new_listings_list,
			op_kwargs = Variable.get("secret_key_rent_analysis_settings", deserialize_json=True),
			provide_context = True
		)	

	email = PythonOperator(
		    task_id='email',
		    python_callable=send_daily_email,
		    provide_context=True,
		    op_kwargs = Variable.get("listings_preferences", deserialize_json=True)
		)

	rent_analysis_etl >> get_new_listings >> email