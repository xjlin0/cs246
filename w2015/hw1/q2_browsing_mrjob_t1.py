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
    pairs  = [ ( pair[0], [ pair ] ) for pair in itertools.permutations(items, 2)]
    return single + pairs

  def reducer_groupByKey(self, key, values):   #groupByKey()
    yield key, list(itertools.chain(*values))  #yield key, reduce(lambda a, b: a + b, values)

  def mapper_aggregate_tops(self, key, all_values):
    items = [ pair[1] for pair in all_values ]
    topCounter = Counter(items)
    total = topCounter['Total']
    del(topCounter['Total'])
    topNarray = topCounter.most_common( topN )
    topNarray.append(('Total', total)) #so the count of single item will always at the end.
    if topNarray and topNarray[-1][1] > s:
      yield key, topNarray #prepare for seperation of methods in the future

  def mapper_store_counters(self, k1, topNarray):
    total = float( topNarray.pop(-1)[1] ) 
    local_counter = Counter()
    for k2, count in topNarray:
      local_counter[(k1, k2)] = count/total
    #get biggiest N element #from heapq import nlargest  #http://stackoverflow.com/a/2243562/4257237  or http://sochor.co/Map-Reduce-Miniproject.html
    topNList = local_counter.most_common(topN) 
    yield k1, topNList

  def combiner_maxN(self, _, topNarray):
    combined = reduce(lambda a, b: a + b, topNarray)
    yield None, Counter(dict((tuple(pair_count[0]), pair_count[1]) for pair_count in combined)).most_common( topN )

  def reducer_maxN(self, _, topNarray):
    combined = reduce(lambda a, b: a + b, topNarray)
    yield None, Counter(dict((tuple(pair_count[0]), pair_count[1]) for pair_count in combined)).most_common( topN )

  def steps(self):
    return [
        MRStep(mapper=self.mapper_parse_line,
               reducer=self.reducer_groupByKey),
        MRStep(mapper=self.mapper_aggregate_tops),
        MRStep(mapper=self.mapper_store_counters,
               combiner=self.combiner_maxN,
               reducer=self.reducer_maxN) ]

if __name__ == '__main__':
    MRProductRecommendation.run()

print("--- %s seconds ---" % (time.time() - start_time))