"""The classic MapReduce job: count the frequency of words and get the most frequent word
   Modified from https://github.com/Yelp/mrjob
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time
start_time = time.time()

WORD_RE = re.compile(r"[\w']+")

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
    #Split each line of the file, count each word as 1 and return the entire List
        return [ (word, 1) for word in WORD_RE.findall(line) ]

    def combiner(self, word, counts):
    #Reduce by using the word as the key. Make the (Null, (word, count)) as output
        yield None, (sum(counts), word)

    def reducer(self, _, wordCountPair):
    #Ignore Null by _ and report the most occured word pair by max(), implicitly the first element(count)
        yield max(wordCountPair)


if __name__ == '__main__':
     MRWordFreqCount.run()

print("--- %s seconds ---" % (time.time() - start_time))