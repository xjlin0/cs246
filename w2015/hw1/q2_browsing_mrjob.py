# Specifically determine input http://stackoverflow.com/questions/26082234
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter
import itertools
#from heapq import nlargest  #http://stackoverflow.com/a/2243562/4257237  or http://sochor.co/Map-Reduce-Miniproject.html
s        = 100 # set support threshold
topN     = 15  # set top items to show
#fileName = 'browsing.txt'
fileName = 'q2testdata.txt'  #toy input set for test

class MRProductRecommendation(MRJob):

  def mapper_parse_line(self, _, line):
    self.maxN = {}
    items  = line.split()
    single = [ ( item, [ ( item, 'Total') ] )  for item in items  ]
    pairs  = [ ( pair[0], [ pair ] ) for pair in itertools.permutations(items, 2)]
    return single + pairs

  def reducer_groupByKey(self, key, values):
    yield key, reduce(lambda a, b: a + b, values)  #groupByKey() or yield (key, list(itertools.chain(*values)))

  def combiner_aggregate_tops(self, key, values):
    topNarray = Counter(values).most_common( topN + 1 )
    if topNarray[0][1] > s
      yield key, topNarray
#What if 100% rate? (('a', 'c'), 45), (('a', 'Total'), 45)
  def mapper_calcSC(self, k1, topNarray):
    total = float( topNarray.pop(0)[1] ) #pop(0) would be (('a', 'Total'), 45)
    yield [ ((k1, k2), count/total) for k2, count in topNarray ]

  def reducer_find_maxN(self, pair, score):
    self.maxN(pair) = score  #add a pair score to the final answer
    self.maxN = Counter( dict( self.maxN.most_common(topN) ) )

  def reducer_final_print_maxN(self):
    print self.maxN

  def steps(self):
    return [
        MRStep(mapper=self.mapper_parse_line,
               reducer=self.reducer_groupByKey,
               combiner=self.combiner_aggregate_tops ),
        MRStep(mapper=self.mapper_calcSC,
               reducer=self.reducer_find_maxN,
               reducer_final=self.final_print_maxN ) ]

if __name__ == '__main__':
    MRProductRecommendation.run()