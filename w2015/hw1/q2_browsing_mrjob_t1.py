# Specifically determine input http://stackoverflow.com/questions/26082234
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter
import itertools
import time
start_time = time.time()

s        = 100 # set support threshold
topN     = 15  # set top items to show

class MRProductRecommendation(MRJob):

  def mapper_parse_line(self, _, line):
    items  = line.split()
    single = [ ( item, [ ( item, 'Total') ] )  for item in items  ]
    pairs  = [ ( pair[0], [ pair ] ) for pair in itertools.permutations(items, 2) ]
    return single + pairs

  def reducer_groupByKey(self, key, values):   #groupByKey()
    yield key, list(itertools.chain(*values))  #yield key, reduce(lambda a, b: a + b, values)

  def mapper_aggregate_tops(self, key, all_values):
    items = [ pair[1] for pair in all_values ]
    topCounter = Counter( items )
    total = topCounter['Total']
    if total > s:
      del( topCounter['Total'] )
      topNarray = topCounter.most_common( topN )
      topNarray.append( ('Total', total) ) #so the count of single item will be always at the end.
      yield key, topNarray

  def mapper_calcCS_byCounter(self, k1, topNarray):
    total = float( topNarray.pop(-1)[1] )
    yield k1, Counter( dict( ((k1, k2), count/total ) for k2, count in topNarray ) ).most_common( topN )

  def reducer_takeOrdered_byCounter(self, _, topNarray): #require both reducer and combiner
    combined = reduce( lambda a, b: a + b, topNarray )
    yield None, Counter( dict( (tuple(pair), count ) for pair, count in combined ) ).most_common( topN )

  def steps(self):
    return [
        MRStep(mapper=self.mapper_parse_line,
               reducer=self.reducer_groupByKey),
        MRStep(mapper=self.mapper_aggregate_tops),
        MRStep(mapper=self.mapper_calcCS_byCounter,
               combiner=self.reducer_takeOrdered_byCounter,
               reducer=self.reducer_takeOrdered_byCounter )    ]

if __name__ == '__main__':
    MRProductRecommendation.run()

print("--- %s seconds ---" % (time.time() - start_time))