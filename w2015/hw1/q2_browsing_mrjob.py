# http://stackoverflow.com/questions/26082234
from mrjob.job import MRJob
from mrjob.step import MRStep
import itertools

s        = 100 # set support threshold
topN     = 15  # set top items to show
#fileName = 'browsing.txt'
fileName = 'q2testdata.txt'  #toy input set for test
singleCount = None

class MRProductRecommendation(MRJob):

    def steps(self):
        return [
            MRStep(mapper_init=self.init_get_single_count,
                   mapper=self.mapper_parse_line,
                   combiner=self.combiner_count_singles,
                   reducer=self.reducer_count_singles,
                   reducer_final=self.final_store_single_count),
            MRStep(mapper=self.mapper_itemSet_count,
                   combiner=self.combiner_count_itemSet,
                   reducer=self.reducer_count_itemSet),
            MRStep(mapper=self.mapper_calcualte_score)
        ]

   ############ Get single item count #####

    def init_get_single_count(self):
        self.single_count = {}

    def mapper_parse_line(self, _, line):
        for single in line.split():
            yield single, 1

    def combiner_count_singles(self, single, counts):
        yield single, sum(counts)

    def reducer_count_singles(self, single, counts):
        yield single, sum(counts)

    def final_store_single_count(self, single, counts):
        if counts > s:
            self.single_count[single]=counts

  #############  Get item pair count ###########
  # Do I need to put ARGV[0] as input?
    def mapper_itemSet_count(self):
        for itemSet in list(itertools.permutations(line.split(),2)):
            yield itemSet, 1

    def combiner_count_itemSet(self, itemSet, setCounts):
        yield itemSet, sum(setCounts)

    def reducer_count_itemSet(self, itemSet, setCounts):
        yield itemSet, sum(setCounts)

########### calcualte confident score   ###########

    def mapper_calcualte_score(self, itemSet, setCounts):
        yield itemSet, float(setCounts)/self.single_count[itemSet[0]]

if __name__ == '__main__':
    MRProductRecommendation.run()