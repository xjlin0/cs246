# Specifically determine input http://stackoverflow.com/questions/26082234
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter 
import itertools

s        = 100 # set support threshold
topN     = 15  # set top items to show

class MRProductRecommendation(MRJob):

  maxN = Counter()

  def mapper_parse_line(self, _, line):
    items  = line.split()
    single = [ ( item, [ ( item, 'Total') ] )  for item in items  ]
    pairs  = [ ( pair[0], [ pair ] ) for pair in itertools.permutations(items, 2)]
    return single + pairs

  def reducer_groupByKey(self, key, values):
    yield key, reduce(lambda a, b: a + b, values)  #groupByKey() or yield (key, list(itertools.chain(*values)))

  def mapper_aggregate_tops(self, k1, all_values):
    items = [pair[1] for pair in all_values]
    topCounter = Counter(items)
    total = topCounter['Total']
    del(topCounter['Total'])
    topNarray = topCounter.most_common( topN )
    topNarray.append(('Total', total)) #so the count of single item will always at the end.
    if topNarray and topNarray[-1][1] > s:
      total = float( topNarray.pop(-1)[1] ) 
      #return key, topNarray #prepare for seperation of methods in the future
      
      for k2, count in topNarray:
        MRProductRecommendation.maxN[(k1, k2)] = count/total
    #get biggiest N element #from heapq import nlargest  #http://stackoverflow.com/a/2243562/4257237  or http://sochor.co/Map-Reduce-Miniproject.html
    topNList = MRProductRecommendation.maxN.most_common(topN) 
    MRProductRecommendation.maxN = Counter( dict(topNList) )
    print MRProductRecommendation.maxN

  def reducer_print_maxN(self):
    print MRProductRecommendation.maxN

  def steps(self):
    return [
        MRStep(mapper=self.mapper_parse_line,
               reducer=self.reducer_groupByKey),
        MRStep(mapper=self.mapper_aggregate_tops) ]

if __name__ == '__main__':
    MRProductRecommendation.run()