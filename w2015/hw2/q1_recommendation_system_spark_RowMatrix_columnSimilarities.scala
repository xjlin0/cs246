

// Copied from https://www.snip2code.com/Snippet/181216/CosineSimilarity-DIMSUM-Example and https://databricks.com/blog/2014/10/20/efficient-similarity-algorithm-now-in-spark-twitter.html
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

val filename = "data.txt"
//val filename = "07-recsys1.txt"

// Arguments for input and threshold
// val filename = args(0)
// val threshold = args(1).toDouble

// Load and parse the data file.
val rows = sc.textFile(filename).map { line =>
  val values = line.split(' ').map(_.toDouble)
  Vectors.dense(values)
}
val mat = new RowMatrix(rows)

// Compute similar columns perfectly, with brute force.
val simsPerfect = mat.columnSimilarities()
println("Pairwise similarities are: " + simsPerfect.entries.collect.mkString(", "))
// Compute similar columns with estimation using DIMSUM, with estimation focusing on pairs more similar than 0.8
// val simsEstimate = mat.columnSimilarities(0.8)

// println("Estimated pairwise similarities are: " + simsEstimate.entries.collect.mkString(", "))

sc.stop()