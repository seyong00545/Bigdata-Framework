from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
import sys, os
#import threading

### Add python kafka producer
from kafka import KafkaProducer

def write_table(dataframe, keys_space_name, table_name):
      dataframe.write\
        .format("org.apache.spark.sql.cassandra")\
	.mode('append')\
        .options(table=table_name, keyspace=keys_space_name)\
        .save()

def data_fusion():
	pass

def check_weight(val):
	print("@@@@weight function start")
	temp = val.split(",")
	sensorID, value , timestamp = temp[0], int(temp[1]), temp[2]
	rawdf = sqlContext.createDataFrame([(sensorID, value, timestamp)],
					   ["sensorID","value","timestamp"])
	df = rawdf.filter("value < 100")

	producer.send('result', str(df.collect()))
	producer.flush()
	
def check_distance(val):
	print("@@@@distance function start")
	temp = val.split(",")
	sensorID, value , timestamp = temp[0],int(temp[1]), temp[2]
	rawdf = sqlContext.createDataFrame([(sensorID, value, timestamp)],
					   ["sensorID","value","timestamp"])
	df = rawdf.filter("value < 10")

	producer.send('result', str(df.collect()))
	producer.flush()

def wdata_handler(rdd):
	records = rdd.collect()
	for record in records:	
		result = record[1]
		check_weight(result)

def ddata_handler(rdd):
	records = rdd.collect()
	for record in records:	
		result = 
		record[1]
		check_distance(result)

### Create a StremingContext with batch interval of 1sec
if __name__ == "__main__":
	
	producer = KafkaProducer(bootstrap_servers='localhost:9092')	

	sc = SparkContext(appName="Data Fusion edge side")
	ssc = StreamingContext(sc,0.5)
	sqlContext = SQLContext(sc)

	### Create kafka streaming with argv(localhost:2181)
	zkQuorum = sys.argv[1]

	ch_weight = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer1", {"weight": 1})
	ch_distance = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer2", {"distance": 1})

	ch_weight.foreachRDD(wdata_handler)
	ch_distance.foreachRDD(ddata_handler)

	### Start the computation
	ssc.start()

	### Wait for the computation to terminate
	ssc.awaitTermination()




















"""
	topic = ["weight","distance"]
	for i in topic:
		thread = threading.Thread(target=setTopic, args=(i,))
		thread.start()


def setTopic(topic):
	if topic=="weight":
		print("@@@@weight thread start")
		ch_weight = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer1", {topic: 1})
		ch_weight.foreachRDD(wdata_handler)

		### Start the computation
		ssc.start()

		### Wait for the computation to terminate
		ssc.awaitTermination()
	if topic == "distance":
		print("@@@@distance thread start")
		ch_weight = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer2", {topic: 1})
		ch_distance.foreachRDD(ddata_handler)
		### Start the computation	
		ssc.start()

		### Wait for the computation to terminate
		ssc.awaitTermination()


"""
