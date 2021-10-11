#!/usr/bin/env python

# based on examples/src/main/python/streaming/kafka_wordcount.py

from __future__ import print_function

import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def returnText(x):
    try:
        return x['text']
    except:
        return ""

if __name__ == "__main__":

    topic = 'twitter-stream'

    sc = SparkContext()
    ssc = StreamingContext(sc, 1) # 1 second intervals!
    ssc.checkpoint("checkpoint")

    zkQuorum = "localhost:2181"
    kafka_stream = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    
    def updateFunc(newvals, oldvals):
        if oldvals is None:
            oldvals = list([0,0,0,0,0])
        nvals = newvals[0]
        
        # 0 stands for previous batches, 1 for new batch, d for decaying, f for fixed
        mean1 = float(nvals[0])
        new_squares = float(nvals[2])
        var1 = float(nvals[3])
        n1 = float(nvals[4])
    
        mean0_d = float(oldvals[0])
        mean0_f = float(oldvals[1])
        var0_d = float(oldvals[2])
        var0_f = float(oldvals[3])
        n0 = float(oldvals[4])
    
        w = n1/(n1+n0)
        w2 = (n0 - 1) / (n0 + n1 - 1)
        w3 = n0 / ((n0 + n1 - 1)**2)

        newmean1 = (1-w)*mean0_d+w*mean1
        newmean2 = (0.8)*mean0_f +0.2*mean1
        newvar1 = w2*var0_d + ((new_squares - (newmean1**2)*(n0 + n1) + n0*(mean0_d**2)) / (n0 + n1 -1)) # Decompose variance in old and new sum of squares, and update old mean to new one to have variance properly
        newvar2 = 0.8*var0_f + 0.2*var1

        return(list([newmean1, newmean2, newvar1, newvar2, (n0+n1)]))

                   
    lines = kafka_stream.map(lambda x: json.loads(x[1])).map(returnText)

    words = lines.map(lambda line: line.split(" ")).map(lambda line: ('value', [len(line), len(line)**2, 1]))\
    .reduceByKey(lambda a, b: [a[0] + b[0], a[1] + b[1], a[2] + b[2]])\
    .map(lambda a: (a[0], list([a[1][0]/a[1][2], a[1][0]/a[1][2], a[1][1], (a[1][1] - (((a[1][0])**2)/a[1][2]))/a[1][2], a[1][2]],)))\
    .updateStateByKey(updateFunc)

    
    words.pprint()
    ssc.start()
    ssc.awaitTermination()
