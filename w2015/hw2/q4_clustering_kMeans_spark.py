# Under the PySpark shell, type:
# execfile('q4_clustering_kMeans_spark.py')

#from pyspark import SparkContext, SparkConf
# conf = SparkConf()
# conf.setMaster("local")
# conf.setAppName("Recommendation System")
# conf.set("spark.executor.memory", "16g")
#sc = SparkContext(conf=conf)

###############################################################################
# NOT MY CODE, modified from Apache Spark Python example of MLlib - Clustering
# http://spark.apache.org/docs/latest/mllib-clustering.html
###############################################################################

from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
#from math import sqrt

# Load and parse the data
#fileName = "data/mllib/kmeans_data.txt"
fileName = "data.txt"
data = sc.textFile(fileName, 8) #partition goes here
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')])).cache()

# Build the models with different seeders: random or fariest spots
c1_clusters = KMeans.train(parsedData, 10, maxIterations=20, runs=1, initializationMode="random")

c2_clusters = KMeans.train(parsedData, 10, maxIterations=20, runs=1, initializationMode='k-means||')

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point, model):
    center = model.centers[model.predict(point)]
    return sum([x**2 for x in (point - center)])**0.5

def wssse(dataRDD, model):
  return dataRDD.map(lambda point: error(point, model)).reduce(lambda x, y: x + y)

c1_WSSSE = wssse(parsedData, c1_clusters)
c2_WSSSE = wssse(parsedData, c2_clusters)

print("\n(c1 random) Within Set Sum of Squared Error = " + str(c1_WSSSE))


## No control of specific seeder or iterations whatsoever.....

second = [float(i) for i in '0.21 0.28 0.5 0 0.14 0.28 0.21 0.07 0 0.94 0.21 0.79 0.65 0.21 0.14 0.14 0.07 0.28 3.47 0 1.59 0 0.43 0.43 0 0 0 0 0 0 0 0 0 0 0 0 0.07 0 0 0 0 0 0 0 0 0 0 0 0 0.132 0 0.372 0.18 0.048 5.114 101 1028 1'.split(' ')]  #this is copy from the second line of data.txt
ans1 = c1_clusters.clusterCenters
print [c1_clusters.predict(ans1[i]) ==  c1_clusters.predict(second) for i in range(10)]
# => [False, False, False, False, False, False, False, False, True, False]
# second document matched to 9th tag!!

print("\n(c2 fariest spots) Within Set Sum of Squared Error = " + str(c2_WSSSE))
ans2 = c2_clusters.clusterCenters
print [c2_clusters.predict(ans2[i]) ==  c2_clusters.predict(second) for i in range(10)]