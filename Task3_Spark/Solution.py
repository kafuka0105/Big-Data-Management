#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext

def mapper(partitionId, records):
	if partitionId == 0:
		next(records)
	import csv
	reader = csv.reader(records)
	for r in reader:
		if (len(r) == 18):
			yield (f'"{r[1].lower()}"' if ',' in r[1] else r[1].lower(), r[0][:4], r[7].lower())

if __name__ == '__main__':
    sc = SparkContext()
    
    complaints = sc.textFile('/tmp/bdm/complaints.csv')
        
    complaints_info = complaints.mapPartitionsWithIndex(mapper)

    complaints_info \
        .map(lambda x: ((x[0],x[1],x[2]), 1)) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda x: ((x[0][0],x[0][1]),x[1])) \
        .groupByKey() \
        .sortByKey() \
        .mapValues(lambda x: (sum(x),len(x),max(x))) \
        .map(lambda x: (x[0][0],x[0][1],x[1][0],x[1][1],round(x[1][2]/x[1][0]*100))) \
        .saveAsTextFile('output')
