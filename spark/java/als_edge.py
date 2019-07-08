from __future__ import print_function

import sys
if sys.version >= '3':
    long = int

import time
from pyspark.ml.recommendation import ALS,ALSModel
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


def recommendProduct(cart):
	startTime = time.time()
	
	virtualCart = spark.createDataFrame(cart)
	a = time.time()
	print("a: "+str(a - startTime))
	productSubset = model.recommendForItemSubset(virtualCart, 1).rdd
	b = time.time()
	print("b: "+str(b - a))
	#first, input : product, output : recommended invoice_num
    
	invoiceTemp = productSubset.first().recommendations[0].invoice_num
	c = time.time()
	print("c: "+str(c - b))
	invoice = spark.createDataFrame([Row(invoice_num = invoiceTemp)])
	d = time.time()
	print("d: "+str(d - c))
	invoiceSubset = model.recommendForUserSubset(invoice, 3)
	e = time.time()
	print("e: "+str(e - d))
	
	output = invoiceSubset.select('recommendations')
	final = time.time()
	print("final: "+str(final - e))
	#output.show(truncate = False)
	return output
	# second, input : invoice_num, output : recommended prouduct

if __name__ == "__main__":
	

	spark = SparkSession\
		.builder\
		.appName("ALSEdge")\
		.getOrCreate()
	spark.sparkContext.setLogLevel('ERROR')

	modelPath = "models/recommendModel"
	model = ALSModel.load(modelPath)
	#testCart = [Row(product_code=4107)] # test : virtual cart
	#recommendProduct(model, [Row(product_code=4107)])
	

	spark.stop()

