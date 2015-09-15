# Under the PySpark shell, type:
# execfile('q1_recommendation_system_spark_t2.py')

#from pyspark import SparkContext, SparkConf
# conf = SparkConf()
# conf.setMaster("local")
# conf.setAppName("Association Rules")
# conf.set("spark.executor.memory", "16g")
#sc = SparkContext(conf=conf)
from collections import Counter
import numpy as np

def center_vector(vector): #[7, 6, 0, 2] => [2, 1, 0, -3] (center only on nonzeros)
  mean = float( sum(vector) ) / np.nonzero(vector)[0].size
  return [0 if item == 0 else item - mean for item in vector]

def pearson_nonzero(list1, list2):
  return np.corrcoef(center_vector(list1), center_vector(list2))[0, 1]

def pearson_nonzero_indexes(indexes):
	return np.corrcoef(center_vector(data(indexes[0])), center_vector(data(indexes[1])))[0, 1]

>>> for pair in itertools.combinations(range(len(data)), 2):
...     nn[pair]=pearson_nonzero_indexes(pair)
...
Traceback (most recent call last):
  File "<stdin>", line 2, in <module>
  File "<stdin>", line 2, in pearson_nonzero_indexes
TypeError: 'numpy.ndarray' object is not callable
>>>

#[(pair, pearson_nonzero(pair)) for pair in itertools.combinations(data, 2)]

def parse_line(line):
	return [ int(item) for item in line.split() ]

fileName = '07-recsys1.txt'
#fileName = 'q1-dataset/q1-dataset/user-shows.txt'
topN = 2

data = np.loadtxt(fileName, delimiter=" ")#.tolist()

#list(itertools.combinations(range(len(data)), 2)) #all indexes in pair to avoid unhashable list problems

#Counter( dict( ((k1, k2), count/total ) for k2, count in topNarray ) ).most_common( topN )

#.collectAsMap() for dictionary

# # CS100.1x lab3_text_analysis_and_entity_resolution_student
# # Part 3: ER as Text Similarity - Cosine Similarity
# def dotprod(a, b):
# 	""" Compute dot product Args:
# 	a (dictionary): first dictionary of record to value
# 	b (dictionary): second dictionary of record to value Returns:
# 	dotProd: result of the dot product with the two input dicti onaries
# 	"""
# 	return sum(value * b.get(key, 0) for key, value in a.items())


# def norm(a):
# 	""" Compute square root of the dot product Args:
# 	a (dictionary): a dictionary of record to value Returns:
# 	norm: a dictionary of tokens to its TF values """
# 	return math.sqrt(sum([value * value for key, value in a.item s()]))


# def cossim(a, b):
# 	""" Compute cosine similarity Args:
# 	a (dictionary): first dictionary of record to value
# 	b (dictionary): second dictionary of record to value Returns:
# 	cossim: dot product of two dictionaries divided by the norm of the first dictionary and
# 	"""
# 	#then by the norm of the second dictionary
# 	return (dotprod(a, b) / norm(a)) / norm(b) testVec1 = {'foo': 2, 'bar': 3, 'baz': 5}
# 	testVec2 = {'foo': 1, 'bar': 0, 'baz': 20}
# 	dp = dotprod(testVec1, testVec2) nm = norm(testVec1)
# 	print dp, nm


# # TODO:Replace<FILLIN>withappropriatecode
# def cosineSimilarity(string1, string2, idfsDictionary):
# 	""" Compute cosine similarity between two strings Args:
# 	string1 (str): first string
# 	string2 (str): second string
# 	idfsDictionary (dictionary): a dictionary of IDF values
# 	Returns:
# 	cossim: cosine similarity value
# 	"""
# 	w1 = tfidf(tokenize(string1), idfsDictionary) w2 = tfidf(tokenize(string2), idfsDictionary) return cossim(w1, w2)
# 	cossimAdobe = cosineSimilarity('Adobe Photoshop', 'Adobe Illustrator',
# 	                               idfsSmallWeights)
# 	print cossimAdobe


# #from sklearn.metrics.pairwise import cosine_similarity
# #cosine_similarity([1, 0, -1], [-1,-1, 0])