# http://stackoverflow.com/questions/26082234
from mrjob.job import MRJob
from mrjob.step import MRStep
from NumLines import NumLines
from WordsPerLine import WordsPerLine
import itertools
import sys

s        = 100 # set support threshold
topN     = 15  # set top items to show
fileName = 'browsing.txt'
#fileName = 'q2testdata.txt'  #toy input set for test
singleCount = None

def firstJob(input_file):
    global singleCount
    mr_job = NumLines(args=[input_file])
    with mr_job.make_runner() as runner:
        runner.run()
        singleCount = runner.get_output_dir()

def secondJob(input_file):
    mr_job = WordsPerLine(args=[intermediate,input_file])
    with mr_job.make_runner() as runner:
        runner.run()

if __name__ == '__main__':
    firstJob(sys.argv[1])
    secondJob(sys.argv[1])



#####################



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