# spark-genome-stats

A simple PySpark project to calculate DNA base frequencies from a FASTA file using different Spark processing techniques.

## 📁 Project Structure

```
├── data/
│ └── sample.fasta # Input FASTA file
├── method1.py # Simple / Not Scalable
├── method2.py # Improved / Scalable
├── method3.py # Best Method
├── run.sh # Runner script to select method
```

## 🧪 What It Does

This project reads a DNA sequence from a FASTA file and calculates the frequency of each base (A, T, G, C). It demonstrates **three different methods** of doing this using PySpark:

- **Method 1:** Emits (base, 1) values for each rdd record, followed by reduceByKey
- **Method 2:** Improves on method1 by calculating (base, freq) for each rdd record, followed by reduceByKey
- **Method 3:** Improves on method2 by using mapPartitions, followed by reduceByKey

## 🧰 Requirements

- Python 3.x
- Apache Spark
- `pyspark` library

Install `pyspark` with:

```bash
pip install pyspark
```
Make sure SPARK_HOME is correctly set.

```bash
export SPARK_HOME=/path/to/spark
export PATH=$SPARK_HOME/bin:$PATH
source ~/.bashrc
```
## 🚀 Run the project

Make `run.sh` executable.
```bash
chmod +x run.sh
```
To run Method 1 : 
```bash
./run.sh 1
```

To run Method 2 : 
```bash
./run.sh 2
```

To run Method 3 : 
```bash
./run.sh 3
```


