# Under the PySpark shell, type:
# execfile('q2_browsing_spark_t1.py')

#from pyspark import SparkContext, SparkConf
# conf = SparkConf()
# conf.setMaster("local")
# conf.setAppName("Association Rules")
# conf.set("spark.executor.memory", "16g")
#sc = SparkContext(conf=conf)
import itertools

s        = 100 # set support threshold
topN     = 15  # set top items to show
fileName = 'browsing.txt'
#fileName = 'q2testdata.txt'  #toy input set for test

def parse_line(line):
  items  = line.split()
  single = [ ( ( item, 'Total'), 1 )  for item in items ]
  pairs  = [ ( pair, 1 ) for pair in itertools.permutations(items, 2)]
  return single + pairs

def sort_filter_arrange(counts): #and put the total in the end
  total, results = 0, sorted(list(counts), key=lambda (key, count): -count)
  for key, count in results:
    if key == 'Total':
      total = count
  if total > s:
    results.remove(('Total', total))
    results.append(('Total', total))
    return results #so Total will be always at the end.

def calcCS(pair):
  k1, counts, total = pair[0], pair[1], 0
  if counts != None:
    total = float( counts.pop(-1)[1] )
    return [ ((k1, k2), count/total) for k2, count in counts ]

confidenceRDD = (sc.textFile(fileName, 12 ) #partition goes here
                  .flatMap( parse_line )
                  .reduceByKey(lambda a, b: a + b)
                  .map(lambda ((key, other), count):(key, (other, count)))
                  .groupByKey()
                  .mapValues( sort_filter_arrange )
                  .filter(lambda (key, values): values != None)
                  .flatMap( calcCS )
                  )

print "Top " + str(topN)
#print confidenceRDD.collect()
print confidenceRDD.takeOrdered(topN, lambda (itemSet, score): -score)