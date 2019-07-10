#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
if sys.version >= '3':
    long = int

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# $example on$
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
# $example off$

def read_table(keys_space_name, table_name):
    table = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ALSExample")\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    
    # $example on$
    #lines = spark.read.text("data/mllib/als/sample_movielens_ratings.txt").rdd
    lines = read_table("torecommend", "dataset").na.replace("''","00000").rdd
    lines.foreach(print)
    #parts = lines.map(lambda row: row.value.split(","))
    parts = lines.map(lambda row: [row.invoice_num, row.product_code])
    #parts.foreach(print)
    ratingsRDD = parts.map(lambda p: Row(invoice_num=int(p[0]), product_code=int(p[1]),
                                         rating=float(1.0)))
    #ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),rating=float(1.0)))
    
    ratings = spark.createDataFrame(ratingsRDD)

    # Create test and train set          ([weight,seed])
    (training, test) = ratings.randomSplit([0.8, 0.2])

    # Build the recommendation model using ALS on the training data
    # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    # Create ALS model
    als = ALS(maxIter=5, regParam=0.01, userCol="invoice_num", itemCol="product_code", ratingCol="rating", coldStartStrategy="drop")
    model = als.fit(training)
    model.write().overwrite().save("models/recommendModel")
    invoiceRecs = model.recommendForAllUsers(10)
    invoiceRecs.show(truncate = False)
    print(als.getItemCol())
    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))
"""
    # Generate top 10 movie recommendations for each user
    invoiceRecs = model.recommendForAllUsers(10)

    # Generate top 10 user recommendations for each movie
    productRecs = model.recommendForAllItems(10)

    # Generate top 10 movie recommendations for 3 users
    invoices = ratings.select(als.getUserCol()).distinct().limit(3)
    invoiceSubsetRecs = model.recommendForUserSubset(invoices, 10)
    # Generate top 10 user recommendations for 3 movies

    products = ratings.select(als.getItemCol()).distinct().limit(3)
    productSubSetRecs = model.recommendForItemSubset(products, 10)
    # $example off$

    #invoiceRecs.show(truncate = False)
    #productRecs.show(truncate = False)
    invoiceSubsetRecs.show(truncate = False)
    productSubSetRecs.show(truncate = False)
    spark.stop()
"""
    
