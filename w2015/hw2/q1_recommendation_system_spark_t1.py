# Under the PySpark shell, type:
# execfile('q1_recommendation_system_spark_t1.py')

#from pyspark import SparkContext, SparkConf
# conf = SparkConf()
# conf.setMaster("local")
# conf.setAppName("Recommendation System")
# conf.set("spark.executor.memory", "16g")
#sc = SparkContext(conf=conf)
# from scipy.spatial import distance
#from scipy.spatial import distance as dt
from collections import Counter
import numpy as np

def center_vector(vector): #[7, 6, 0, 2] => [2, 1, 0, -3] (center only on nonzeros)
  mean = float( sum(vector) ) / np.nonzero(vector)[0].size
  return [0 if item == 0 else item - mean for item in vector]

def pearson_nonzero(list1, list2):
  return np.corrcoef(center_vector(list1), center_vector(list2))[0, 1]

# def pearson_nonzero_indexes(indexes):
# 	return np.corrcoef(center_vector(data(indexes[0])), center_vector(data(indexes[1])))[0, 1]

def map_tops(row, topN=2):
  #input: [0.41403934 , -0.17854212,  -2, -0.10245014, -0.30895719,  0.58703951] #output:[user_id,[(top_associtated_user_id, top_similarity),...] ]
  user_id = [index for index, similarity in enumerate(row) if similarity == -2][0]
  return user_id, Counter( dict( enumerate(row) ) ).most_common(topN)


def parse_line(line):
  return [ int(item) for item in line.split() ]

#fileName = 'q1-dataset/q1-dataset/user-shows.txt'
fileName = '07-recsys1.txt'
topN     = 2
dataRDD = (sc.textFile(fileName, 12 ) #partition goes here
             .map( parse_line )
             .zipWithIndex() #([data, data, data], line#)
          )

suggestRDD = (dataRDD
              .cartesian( dataRDD ) #get all possible permutations
              .filter(lambda (object1, object2): object1[1] != object2[1]) #remove self-self combination
              .map(lambda (object1, object2): (object1[1], ( (object1[1], object2[1]),  pearson_nonzero(object1[0], object2[0]) ) ) )  #(0, ((0, 1), 0.57735026918962584))
              .groupByKey()
              .mapValues(lambda similarities: Counter(dict(similarities)).most_common( topN ) )
              # forgot to include original data in the map after filter!!
              )

# Counter( dict( key, count/total ) for k2, count in topNarray ) ).most_common( topN )

#print dataRDD
print suggestRDD.take(2)
#print suggestRDD.count()

# >>> data
# array([[ 1.,  0.,  3.,  0.,  0.,  5.,  0.,  0.,  5.,  0.,  4.,  0.],
#        [ 0.,  0.,  5.,  4.,  0.,  0.,  4.,  0.,  0.,  2.,  1.,  3.],
#        [ 2.,  4.,  0.,  1.,  2.,  0.,  3.,  0.,  4.,  3.,  5.,  0.],
#        [ 0.,  2.,  4.,  0.,  5.,  0.,  0.,  4.,  0.,  0.,  2.,  0.],
#        [ 0.,  0.,  4.,  3.,  4.,  2.,  0.,  0.,  0.,  0.,  2.,  5.],
#        [ 1.,  0.,  3.,  0.,  3.,  0.,  0.,  2.,  0.,  0.,  4.,  0.]])

# similarities = dt.squareform( 1 - dt.pdist([center_vector(row) for row in data], 'cosine') )
# similarities -= np.eye(len(similarities))*2  #use -2 as the label of user_id since other data type is not allowed in the array

# array([[-2         , -0.17854212,  0.41403934, -0.10245014, -0.30895719,  0.58703951],
#        [-0.17854212, -2         , -0.52623481,  0.46800784,  0.39891072, -0.30643976],
#        [ 0.41403934, -0.52623481, -2         , -0.62398065, -0.28426762,  0.50636968],
#        [-0.10245014,  0.46800784, -0.62398065, -2         ,  0.4587349 , -0.23533936],
#        [-0.30895719,  0.39891072, -0.28426762,  0.4587349 , -2         , -0.21591676],
#        [ 0.58703951, -0.30643976,  0.50636968, -0.23533936, -0.21591676, -2        ]])

# associate_users = [ map_tops(row) for row in similarities ]
# [(0, [(5, 0.58703950856427412), (2, 0.41403933560541262)]),
#  (1, [(3, 0.46800784077976632), (4, 0.39891071573694159)]),
#  (2, [(5, 0.50636968354183332), (0, 0.41403933560541262)]),
#  (3, [(1, 0.46800784077976632), (4, 0.45873490213598345)]),
#  (4, [(3, 0.45873490213598345), (1, 0.39891071573694159)]),
#  (5, [(0, 0.58703950856427412), (2, 0.50636968354183332)])]

#zip(associate_users, data)
# [((0, [(5, 0.58703950856427412), (2, 0.41403933560541262)]),
# 	array([ 1.,  0.,  3.,  0.,  0.,  5.,  0.,  0.,  5.,  0.,  4.,  0.])),
# ((1, [(3, 0.46800784077976632), (4, 0.39891071573694159)]),
# 	array([ 0.,  0.,  5.,  4.,  0.,  0.,  4.,  0.,  0.,  2.,  1.,  3.])),
# ((2, [(5, 0.50636968354183332), (0, 0.41403933560541262)]),
# 	array([ 2.,  4.,  0.,  1.,  2.,  0.,  3.,  0.,  4.,  3.,  5.,  0.])),
# ((3, [(1, 0.46800784077976632), (4, 0.45873490213598345)]),
# 	array([ 0.,  2.,  4.,  0.,  5.,  0.,  0.,  4.,  0.,  0.,  2.,  0.])),
# ((4, [(3, 0.45873490213598345), (1, 0.39891071573694159)]),
# 	array([ 0.,  0.,  4.,  3.,  4.,  2.,  0.,  0.,  0.,  0.,  2.,  5.])),
# ((5, [(0, 0.58703950856427412), (2, 0.50636968354183332)]),
# 	array([ 1.,  0.,  3.,  0.,  3.,  0.,  0.,  2.,  0.,  0.,  4.,  0.]))]





#list(itertools.combinations(range(len(data)), 2)) #all indexes in pair to avoid unhashable list problems
#[(pair, pearson_nonzero(pair)) for pair in itertools.combinations(data, 2)] #for Spark base on index?

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