# in PySpark shell
# execfile('WordCount_spark.py')
import re

WORD_RE = re.compile(r"[\w']+")

def parseLine(line):
  return [(word.lower(), 1) for word in WORD_RE.findall(line)]

fileName = 'pg100.txt'

wordRDD = (sc
            .textFile( fileName )
            .flatMap( parseLine )
            .reduceByKey(lambda a, b: a + b)
            #use .cache() for huge data
           )

print wordRDD.max( key=lambda (word, counts): counts )