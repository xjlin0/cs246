# Under the PySpark shell, type:
# execfile('q2_browsing_spark.py')
import itertools

def parseSingle(line):


def parseSet(line, itemSetSize=2):
    return [((itemSet[0], itemSet), 1) for itemSet in list(itertools.permutations(line.split(), itemSetSize))]

s        = 100 # set support threshold
topN     = 15  # set top items to show
fileName = 'browsing.txt'
#fileName = 'q2testdata.txt'  #toy input set for test

fileRDD = sc.textFile(fileName, 8)

singleCountRDD = (fileRDD
                  .flatMap(parseSingle)
                  .reduceByKey(lambda a, b: a + b)
                  .filter(lambda (single, count): count > s)
                  .cache()
                  )

confidence2RDD = (fileRDD
                  .flatMap(lambda line: parseSet(line, itemSetSize=2)) #change to 3 for itemsets size 3?
                  .reduceByKey(lambda a, b: a + b)
                  .map(lambda ((single, itemSet), setsCount): (single, (itemSet, float(setsCount))))
                  .join(singleCountRDD)
                  .map(lambda (single, ((itemSet, setsCount), singleCount)): (itemSet, setsCount / singleCount))
                  .cache()
                  )

print "Top " + str(topN)
print confidence2RDD.takeOrdered(topN, lambda (itemSet, score): -score)