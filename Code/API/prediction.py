import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer,VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark import SparkFiles
from pyspark.ml.regression import RandomForestRegressor,RandomForestRegressionModel
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import Row
from collections import OrderedDict

def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))

def get_prediction(dta):
	spark = SparkSession.builder.appName("HW2_Submission").getOrCreate()
	df = pd.DataFrame(dta,index=[0])
	ddf = spark.createDataFrame(df)
	df = ddf
	print(df.show())
	df = df.withColumn("open", df["open"].cast("double"))
	df = df.withColumn("close", df["close"].cast("double"))
	df = df.withColumn("high", df["high"].cast("double"))
	df = df.withColumn("low", df["low"].cast("double"))
	df = df.withColumn("volume", df["volume"].cast("double"))
	df = df.withColumn('change',df["close"]-df["open"])
	df = df.withColumn('label',df["close"])
	df.show()
	input_cols = ['open', 'high', 'low', 'close', 'volume','sentiment_score']
	assembler = [VectorAssembler(inputCols=input_cols, outputCol ="features")]
	pipeline = Pipeline(stages=assembler)
	df = pipeline.fit(df).transform(df)
	result = df
	res = result.select("label","features")

	
	sameModel = RandomForestRegressionModel.load("stock-model.model")
	predictions = sameModel.transform(res)
	pred = predictions.select("prediction", "label")
	print(pred)
	return pred.toPandas().to_dict(orient='list')
def get_prediction_on_open(openp):
	client = MongoClient()
	client = MongoClient('mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb')
	mydb = client["new6"]
	mycol = mydb["stocks"]
	data = mycol.find_one({'close':openp,'SYM':'DJI'})
	p = get_prediction(data)

	prediction = 0
	if len(p['prediction']) !=0 :
		prediction = p['prediction'][0]
	final = {}
	final['prediction'] = prediction
	final['change_%'] = 100*(float(prediction) - float(data['close']))/float(data['close'])
	final['open'] = openp
	final['SYM'] = 'DJI'
	final['data'] = data
	return final

