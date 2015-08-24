from mrjob.job import MRJob
from mrjob.step import MRStep
import itertools

s        = 100 # set support threshold
topN     = 15  # set top items to show
fileName = 'browsing.txt'
#fileName = 'q2testdata.txt'  #toy input set for test

class MRProductRecommendation(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    def mapper_parse_line(self, _, line):
        # yield each word in the line
        return [(single, 1) for single in line.split()]


    def combiner_count_singles(self, word, counts):
        # optimization: sum the words we've seen so far
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.


    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word



if __name__ == '__main__':
    MRProductRecommendation.run()