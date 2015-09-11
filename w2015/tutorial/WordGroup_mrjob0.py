"""The classic MapReduce job: Group of words
   Modified from https://github.com/Yelp/mrjob
   Goal: to crete groupByKey() similar to Apach Spark
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import itertools
import re
import time
start_time = time.time()

WORD_RE = re.compile(r"[\w']+")

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), ['uno']) #value is wrapped in List, original was  yield (word.lower(), 1)

    def reducer(self, word, counts):
        yield (word, list(itertools.chain(*counts)))  #groupByKey(), reduce(lambda a, b: a + b, counts) can be also used. Original was yield (word, sum(counts))

if __name__ == '__main__':
     MRWordFreqCount.run()

print("--- %s seconds ---" % (time.time() - start_time))