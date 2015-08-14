#[(pair, 1) for pair in list(itertools.combinations(line.split(),2))]
#fileName = 'testb.txt'
import itertools

def parseBrowsingLine(line):
  return [(pair, 1) for pair in list(itertools.permutations(line.split(),2))]

s = 100
topN = 15
#fileName = 'browsing.txt'
fileName = 'q2testdata.txt'
browsingRDD = (sc
                  .textFile( fileName )
                  .flatMap( parseBrowsingLine )
                  .reduceByKey( lambda a, b: a + b )
                  .filter(lambda (pair, counts): counts > s)
                  .cache()
                   )


print "Top " + str(topN)
print browsingRDD.takeOrdered(topN, lambda (pair, counts): -counts)