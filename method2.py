import sys
import time

from pyspark.sql import SparkSession
from collections import defaultdict

def process_record(rdd_rec):
	if rdd_rec[0] == '>':
		return [("z", 1)]
	
	mp = defaultdict(int)
	rec = rdd_rec.lower()
	for c in rec:
		mp[c] += 1
	
	list = [(k,v) for k, v in mp.items()]
	return list	


if __name__ == "__main__":
	if len(sys.argv) != 2 :
		print(f"Usage: {__file__} <inputfile>", file=sys.stderr)
		exit(-1)

	start_time = time.time()	

	spark = SparkSession\
		.builder\
		.appName("method2")\
		.getOrCreate()
			
	input_path = sys.argv[1]
	
	records = spark.sparkContext.textFile(input_path)
	
	pairs = records.flatMap(process_record)
	
	frequencies = pairs.reduceByKey(lambda x, y : x + y)
	
	freq_list = frequencies.collect()
	
	spark.stop()

	end_time = time.time()

	print(f"Frequencies : {freq_list}")
	print(f"Time Taken : {end_time - start_time}")
