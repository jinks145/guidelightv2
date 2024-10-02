import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from polygon import StocksClient
from requests import BadResponse
import pandas as pd
import os, datetime
import pymongo
client = StocksClient("eJJvSZwdjo10d1amvxCBhTk4p0gx_DTk")





with DAG(
	dag_id= "fetch_stock_data",
	start_date=datetime.datetime.now(),
	# schedule_interval="@daily"
) as dag:
	def collect_polygon_tickers():
		tickers = []
		for ticker in client.list_tickers(limit=100):
				tickers.append(ticker.ticker)

		return tickers
	def transform_tickers(**kwargs):
		ti = kwargs['ti']
		tickers = ti.xcom_pull(task_ids='extract_data')

		collected = []
		for ticker in tickers:
			try:
				res = client.get_previous_close_agg(ticker)
				print(res)
				if len(res) != 0:
					collected.append(res[0].__dict__)
			except BadResponse:
				print(f"failed to get {ticker}")

		
		
		return collected
	def load_stock_data(**kwargs):
		ti = kwargs['ti']
		collected = ti.xcom_pull(task_ids='transform_data')
		print(type(collected), collected)
		df = pd.DataFrame(collected)
		# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
		# output_folder = os.path.join(project_root, "daily_aggregates")
    
		# os.makedirs(output_folder, exist_ok=True)
		# fname = datetime.date.today().strftime('%Y-%m-%d') + ".csv"
		# df.to_csv(os.path.join(output_folder,fname))
		# print(df)
		engine = create_engine('sqlite:/// trades.db', echo=True)
		with engine.connect() as conn:
			df.t('option', )

	extract_data = PythonOperator(
		task_id='extract_data',
		dag=dag,
		python_callable=collect_polygon_tickers,
	)
	transform_data = PythonOperator(
		task_id='transform_data',
		dag=dag,
		python_callable=transform_tickers,
		provide_context=True
	)
	load_data = PythonOperator(
		task_id='load_data',
		dag=dag,
		python_callable=load_stock_data,
		provide_context=True
	)
	

	extract_data >> transform_data >> load_data

		

