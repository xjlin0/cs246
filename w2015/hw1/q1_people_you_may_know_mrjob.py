from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter
import itertools
import time

start_time = time.time()

class MRFriendYouMayKnow(MRJob):

  def mapper_connecteds_and_commons(self, _, line):
    minimum = -9999999999
    user, friends = line.split('\t')
    friends = friends.split(',')
    connecteds = [((user, friend), minimum) for friend in friends]
    commons = [(pair, 1) for pair in itertools.permutations(friends, 2)]
    return connecteds + commons

  def reducer_count_friends(self, pair, counts):
      yield pair, sum(counts)

  def mapper_filter_rearrange(self, pair, counts):
    if counts > 0:
      user, friend = pair[0], pair[1]
      yield user, [ (counts, friend) ] #wrap in List so it can be added to each other

  def reducer_groupByKey(self, key, values):   #groupByKey()
    yield key, list(itertools.chain(*values))  #yield key, reduce(lambda a, b: a + b, values)

  def mapper_suggestion(self, user, potentials):
    N = 10  #only ouput 10 most possible friends
    yield user, Counter( dict( (friend, count) for count, friend in potentials ) ).most_common( N )

  def steps(self):
    return [
        MRStep(mapper=self.mapper_connecteds_and_commons,
               reducer=self.reducer_count_friends ),
        MRStep(mapper=self.mapper_filter_rearrange,
               reducer=self.reducer_groupByKey ),
        MRStep(mapper=self.mapper_suggestion )  ]

if __name__ == '__main__':
    MRFriendYouMayKnow.run()

print("--- %s seconds ---" % (time.time() - start_time))