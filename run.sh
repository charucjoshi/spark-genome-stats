#!/bin/bash

METHOD=$1

INPUT_PATH="./data/sample.fasta"
PROG="./method$METHOD.py"

$SPARK_HOME/bin/spark-submit $PROG $INPUT_PATH
