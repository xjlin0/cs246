# In jenv exec ruby-spark shell
# load('q2_browsing_spark_t2.rb')

require 'ruby-spark'

parseSingle = lambda do |line|
  line.split.map{ |single| [single, 1] }
end

parseSet = lambda do |line|
  line.split.permutation(2).map{ |itemSet| [itemSet, 1] }
end

s        = 100 # set support threshold
topN     = 15  # set top items to show
fileName = 'browsing.txt'
#fileName = 'q2testdata.txt'  #toy input set for test

fileRDD = $sc.textFile( fileName )

singleCountHash = fileRDD
                  .flatMap( parseSingle )
                  .reduceByKey( lambda{ |a, b| a + b } )
                  .filter( "lambda{ |pair| pair.last > %d }" % s )
                  .collect_as_hash   #RDD.collectAsMap() equivalent
                  #no RDD.join available.
singleCountHash.default = 9999999999  #for covering no key condition, divided by nil/0 errors
confidence2RDD  = fileRDD
                  .flatMap( parseSet )
                  .reduceByKey( lambda{ |a, b| a + b } )
                  .map( lambda{ |itemSet, setsCount| [ itemSet, setsCount.to_f / singleCountHash[itemSet[0]] ] } )
                  .bind(singleCountHash: singleCountHash)# => bind is required for accessing singleCountHash within lambda!!

puts "Top " + topN.to_s
# Currently there's no RDD.takeOrdered() in Ruby-spark. Consider using #max(topN) or #max_by(topN) instead
#print confidence2RDD.sort_by(lambda{|itemSet, score| -score}).take(topN)
print confidence2RDD.reduce( lambda{|memo, item| memo[1] > item[1] ? memo : item } ).to_s # customization from #max for taking the item pair with the max score because Ruby-Spark don't have max_by yet.