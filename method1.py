import sys
import time
from pyspark.sql import SparkSession

def process_record(rdd_rec):
	key_value_list = []
	if rdd_rec[0] == '>':
		key_value_list.append(("z", 1))
	else:
		rec = rdd_rec.lower()
		for c in rec:
			key_value_list.append((c, 1))
	return key_value_list

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print(f"Usage: {__file__}, <input_path>", file = sys.stderr)
		exit(-1)
	
	start_time = time.time()
	
	spark = SparkSession\
		.builder\
		.appName("list_method")\
		.getOrCreate()
	
	input_path = sys.argv[1]

	records = spark.sparkContext.textFile(input_path)
	
	pairs = records.flatMap(lambda x : process_record(x))

	frequencies = pairs.reduceByKey(lambda x, y : x + y)

	freq_list = frequencies.collect()
	
	spark.stop()
		
	end_time = time.time()
	print(f"Frequencies : {freq_list}")
	print(f"Time Taken : {end_time - start_time}")
	
