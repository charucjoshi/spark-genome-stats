import sys
import time

from pyspark.sql import SparkSession
from collections import defaultdict

def process_partition(iterator):
	mp = defaultdict(int)
	for rdd_rec in iterator:
		if rdd_rec[0] == '>':
			mp["z"] += 1
		else:
			rec = rdd_rec.lower()
			for c in rec:
				mp[c] += 1
	
	list = [(k, v) for k, v in mp.items()]
	return list

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print(f"Usage: {__file__}, <inputpath>", file=sys.stderr)
		exit(-1)

	start_time = time.time()
	
	spark = SparkSession\
		.builder\
		.appName("method3")\
		.getOrCreate()

	input_path = sys.argv[1]
	
	records = spark.sparkContext.textFile(input_path)
	
	pairs = records.mapPartitions(process_partition)	

	frequencies = pairs.reduceByKey(lambda x, y: x+y)

	freq_list = frequencies.collect()
	
	spark.stop()
	
	end_time = time.time()

	print(f"Frequencies : {freq_list}")
	print(f"Time Taken : {end_time - start_time}")

	
