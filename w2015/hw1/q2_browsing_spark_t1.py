# Under the PySpark shell, type:
# execfile('q2_browsing_spark_t1.py')
import itertools

def parse_line(line):
  items  = line.split()
  single = [ ( ( item, 'Total'), 1 )  for item in items ] #use -99999 as label of 'Total'
  pairs  = [ ( pair, 1 ) for pair in itertools.permutations(items, 2)]
  return single + pairs

def sort_arrange(counts): #and put the total in the end
	total, result = 0, sorted(list(counts), key=lambda (key, count): -count)
	for key, count in result:
		if key is 'Total':
			total = count
	return result.remove(('Total', total)).append(('Total', total)) 


def calcCS(pair):
	k1, counts, total = pair[0], pair[1], 0
	total = float( counts.pop(-1)[1] )
	print "Jack"
	print counts
	return [ ((k1[0], k2), count/total) for k2, count in counts ]

s        = 1 # set support threshold
topN     = 1  # set top items to show
#fileName = 'browsing.txt'
fileName = 'q2testdata.txt'  #toy input set for test

confidenceRDD = (sc.textFile(fileName)
                  .flatMap( parse_line )
                  .reduceByKey(lambda a, b: a + b)
                  .map(lambda ((key, other), count):(key, (other, count)))
                  .groupByKey()
                  .mapValues(lambda counts: sorted(list(counts), key=lambda (key, count): -count))
                  .filter(lambda (key, counts): counts[0][1] > s) #Why -99999 guarantee first place?
            			.flatMap( calcCS )
                  )

print "Top " + str(topN)
print confidenceRDD.collect()
#print confidenceRDD.takeOrdered(topN, lambda (itemSet, score): -score)