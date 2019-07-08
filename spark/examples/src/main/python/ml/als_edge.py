from __future__ import print_function

import sys
if sys.version >= '3':
    long = int

import time
from pyspark.ml.recommendation import ALS,ALSModel
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from kafka import KafkaConsumer, KafkaProducer

############### Check remark please ###############

def recommendProduct(message):
	virtualCart = spark.createDataFrame([Row(product_code = message)])
	productSubset = model.recommendForItemSubset(virtualCart, 1).rdd
	    
	invoiceTemp = productSubset.first().recommendations[0].invoice_num
	invoice = spark.createDataFrame([Row(invoice_num = invoiceTemp)])
	invoiceSubset = model.recommendForUserSubset(invoice, 3)
	outputProduct = []
	for i in range(3):
		outputProduct.append(invoiceSubset.select('recommendations').first().recommendations[i].product_code)
	
	kafkaProducer(outputProduct)

def kafkaConsumer():
	# Input message form : "user_no,product_code"
	for message in consumer:
		print("%s"%(message.value))
		if len(message.value)>0:		
			print(recommendProduct((message.value).decode()))

def kafkaProducer(outputProduct):
	# Output message form : "[user_no,[product_code1,product_code2,product_code3]]" ==> Recommend 3 product
	producer.send('spark-ws', (str([outputProduct])).encode())
	producer.flush()


if __name__ == "__main__":
	consumer = KafkaConsumer('ws-spark',bootstrap_servers='220.67.124.122:9092') #'220.67.124.122:9092'
	producer = KafkaProducer(bootstrap_servers='220.67.124.122:9092')
		
	spark = SparkSession\
		.builder\
		.appName('ALS_Edge')\
		.getOrCreate()
	spark.sparkContext.setLogLevel('ERROR')

	modelPath = "models/recommendModel" # have to configure model path
	model = ALSModel.load(modelPath)
	kafkaConsumer()
	spark.stop()

