# Under the PySpark shell, type:
# execfile('q1_recommendation_system_spark_t2.py')

#from pyspark import SparkContext, SparkConf
# conf = SparkConf()
# conf.setMaster("local")
# conf.setAppName("Association Rules")
# conf.set("spark.executor.memory", "16g")
#sc = SparkContext(conf=conf)
# from scipy.spatial import distance
from scipy.spatial import distance as dt
from collections import Counter
import numpy as np

def center_vector(vector): #[7, 6, 0, 2] => [2, 1, 0, -3] (center only on nonzeros)
  mean = float( sum(vector) ) / np.nonzero(vector)[0].size
  return [0 if item == 0 else item - mean for item in vector]

def map_tops(row, topN=2):
  #input: [0.41403934 , -0.17854212,  -2, -0.10245014, -0.30895719,  0.58703951] #output:[user_id,[(top_associtated_user_id, top_similarity),...] ]
  user_id = [index for index, similarity in enumerate(row) if similarity == -2][0]
  return user_id, Counter( dict( enumerate(row) ) ).most_common(topN)

def suggest_by(topN_similarities, original_row):
	filled = []
	print "\nline 26: ", topN_similarities, original_row
	for outer_index, value in enumerate(original_row):
		if value == 0:
			recommendations = [ data[index][outer_index]*similarity for index, similarity in topN_similarities]
			filled += [ reduce(lambda a, b: a+b, recommendations[1:], recommendations[0]) ]
		else:
			filled += [ value ]
	return filled


fileName = 'q1-dataset/q1-dataset/user-shows.txt'
#fileName = '07-recsys1.txt'
data = np.loadtxt(fileName, delimiter=" ").T#.tolist()
# data = array([[ 1.,  0.,  3.,  0.,  0.,  5.,  0.,  0.,  5.,  0.,  4.,  0.],...])


similarities = dt.squareform( 1 - dt.pdist([center_vector(row) for row in data], 'cosine') )
similarities -= np.eye(len(similarities))*2  #use -2 as the label of user_id since other data type is not allowed in the array
# similarities = array([[-2 , -0.17854212,  0.41403934, -0.10245014, -0.30895719,  0.58703951],..])


associate_users = [ map_tops(row) for row in similarities ]
# associate_users = [(0, [(5, 0.58703950856427412), (2, 0.41403933560541262)]),...] 

#zip(associate_users, data) = 
#[((0, [(5, 0.58703950856427412), (2, 0.41403933560541262)]), 
#	array([ 1.,  0.,  3.,  0.,  0.,  5.,  0.,  0.,  5.,  0.,  4.,  0.])),...] 

recommended = [suggest_by(id_similarities[1], original_row) for id_similarities, original_row in zip(associate_users, data)]
print recommended[499]