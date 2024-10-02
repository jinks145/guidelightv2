# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import yfinance as yf
from polygon.rest import RESTClient


# %%
from pyspark.sql import SparkSession
import datetime

print("=== Populating data from polygon.io ===")
# %%
companies = pd.read_csv('blue_chip_companies_full.csv')

# %%
def fetch_option_contracts(underlying_ticker:str, expired=False, d=30):
	try:
		oclient = RESTClient('eJJvSZwdjo10d1amvxCBhTk4p0gx_DTk')
		result = list(oclient.list_options_contracts(underlying_ticker, expiration_date_gte= (datetime.datetime.today() - datetime.timedelta(days=d)).strftime('%Y-%m-%d'),limit=1000, expired=expired))
		df = pd.DataFrame([vars(contract) for contract in result])
		
		# return df
		return df.get(['contract_type', 'exercise_style', 'expiration_date', 'strike_price', 'ticker', 'underlying_ticker']).to_dict(orient='records')
		
	except Exception as e:
		print(underlying_ticker)
		print(f'failed to get contract for {underlying_ticker} due to {e}')


# %%
import hashlib
def get_contract_aggseries(ticker:str, time_points=30):
	try:
		oclient = RESTClient('eJJvSZwdjo10d1amvxCBhTk4p0gx_DTk')
		results = list(oclient.get_aggs(ticker, multiplier=1, timespan="day", from_= datetime.datetime.today() - datetime.timedelta(days=time_points), to= datetime.datetime.today(),limit=time_points))
		if results:
			df = pd.DataFrame([vars(res) for res in results])
			
			df["agg_id"] = df['timestamp'].apply(lambda x: hashlib.md5(str(x).encode('utf-8')).hexdigest())
			return df.to_dict(orient='records')
		
		return []
	
	except Exception as e:
		print(e)
		print(f'failed to get contract for {ticker} due to \"{e}\"')





# %%
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType, LongType


# %%
from pyspark.sql.functions import udf, explode


# %%
spark = SparkSession.builder \
    .appName("transformation task") \
	.master('local[*]')\
	.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.1.1") \
    .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=10000&appName=mongosh+2.3.0") \
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=10000&appName=mongosh+2.3.0") \
    .config("spark.jars", "./postgresql-42.2.23.jar") \
    .getOrCreate()

# %%
aggs_schema = ArrayType(StructType([
    StructField("open", FloatType(), False),
    StructField("high", FloatType(), False),
	StructField("close", FloatType(), False),
	StructField('volume', FloatType(), False),
    StructField("vwap", FloatType(), False),
	StructField("timestamp", LongType(), False),
	StructField("aggsID", StringType(), True)
    # Add other fields returned by the API if needed
]))

# %%
schema = ArrayType(StructType([
    StructField("ticker", StringType(), True),
    StructField("contract_type", StringType(), True),
	StructField("exercise_style", StringType(), True),
	StructField('expiration_date', StringType(), True),
    StructField("strike_price", FloatType(), True),
    # Add other fields returned by the API if needed
]))

# %%
@udf(aggs_schema)
def get_aggseries_udf(ticker):
    df = get_contract_aggseries(ticker)
    return df if df else []





# %%
@udf(schema)
def fetch_options_udf(symbol):
    return fetch_option_contracts(symbol)

print("=== Get company tickers ===")
# %%
symbols = pd.read_csv('blue_chip_companies_full.csv')['Ticker']


# %%
print("=== Data collection for option contract===")
# Convert the list of symbols into a Spark DataFrame
symbols_df = spark.createDataFrame(symbols.to_frame(), ['underlying_ticker'])

# Apply the UDF to each symbol to fetch options data concurrently
symbols_df['underlying_ticker']


# %%
options_df = symbols_df.withColumn("options_data", fetch_options_udf("underlying_ticker"))
options_df.cache()
# Explode the nested array of options_data into individual rows
final_df = options_df.selectExpr("underlying_ticker", "explode(options_data) as option")


# %%
exploded_df = final_df.select(
    "underlying_ticker",
	"option.ticker",
    "option.contract_type",
    "option.exercise_style",
    "option.expiration_date",
    "option.strike_price",
)

print("=== Data collection for option contract aggregates===")
# %%
aggs_df = exploded_df.filter(exploded_df['ticker'].isNotNull()).withColumn('aggs', get_aggseries_udf('ticker'))
aggs_df.cache()

# %%
from pyspark.sql.functions import struct, expr, col
mongo_df = aggs_df.select(
    "underlying_ticker",
	"ticker",
    "contract_type",
    "expiration_date",
    "strike_price",
	expr("transform(aggs, x -> struct(x.timestamp as timestamp, x.aggsID as aggsID))").alias("aggs")
).repartition(16)
sql_df = aggs_df.withColumn("aggs", explode("aggs")).select(
    "aggs.aggsID",
    "aggs.open",
    "aggs.high",
    "aggs.close",
    "aggs.volume",
    "aggs.vwap",
    "aggs.timestamp"
).repartition(16)
# %%
# %%
print("\n=== Aggregate Schema === \n")
aggs_df.printSchema()

print("=== Writing to mongodb ===\n")
# %%
mongo_df.write.format("mongodb")\
	.mode('append')\
	.option("database", "test")\
	.option("collection", "option_aggs").save()


# %%
print("\n=== Writing to postgres ===")
props = {
    "user": "brad",
    "driver": "org.postgresql.Driver"
}
tablename = "aggregate_heap"
sql_df.write.mode("append").option("batchsize", "10000").jdbc("jdbc:postgresql://localhost:5432/aggstest", tablename, properties=props).save()


# %%
spark.stop()



# %%
print("Spark has completed the task.")