# Under the PySpark shell, type:
# execfile('q2_browsing_spark.py')
import itertools

def parseSingle(line):
    return [ (single, 1) for single in line.split() ]

def parseSet(line, itemSetSize=2):
    return [ ((itemSet[0], itemSet), 1) for itemSet in list(itertools.permutations(line.split(), itemSetSize))]

s        = 100 # set support threshold
topN     = 15  # set top items to show
fileName = 'browsing.txt'
#fileName = 'q2testdata.txt'  #toy input set for test

fileRDD = sc.textFile(fileName, 8)

singleCountRDD = (fileRDD
                  .flatMap(parseSingle)
                  .reduceByKey(lambda a, b: a + b)
                  .filter(lambda (single, count): count > s)
                  .cache()    #or collectAsMap()
                  )

confidence2RDD = (fileRDD
                  .flatMap(lambda line: parseSet(line, itemSetSize=2)) #change to 3 for itemsets size 3?
                  .reduceByKey(lambda a, b: a + b)  #attach a filter here to reduce memory usage if needed
                  .map(lambda ((single, itemSet), setsCount): (single, (itemSet, setsCount)))
                  .join(singleCountRDD)
                  .map(lambda (single, ((itemSet, setsCount), singleCount)): (itemSet, float(setsCount) / singleCount)) 
                  .cache()   #or lambda (itemSet, setsCount): float(setsCount)/dictionary[ itemSet ], and dictionary[ itemSet ] is disposible now
                  )

print "Top " + str(topN)
print confidence2RDD.takeOrdered(topN, lambda (itemSet, score): -score)