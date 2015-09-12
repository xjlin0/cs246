# In jenv exec ruby-spark shell
# load('q2_browsing_spark_t1.rb')

require 'ruby-spark'

topN     = 15  # set top items to show
fileName = 'browsing.txt'
#fileName = 'q2testdata.txt'  #toy input set for test

sort_filter = lambda do |counts|
  s      = 100 # set support threshold
  results = counts.sort_by{ |key, count| -count }
  total = results.find{ |key, count| key == "Total"}.last
  results << results.delete(["Total", total]) if total > s
end #so Total will be always at the end.

singles_and_pairs = lambda do |line|
  items  = line.split
  singles= items.map{ |item| [[item, 'Total'], 1] }
  pairs  = items.permutation(2).map{ |pair| [pair, 1] }
  singles + pairs
end

calcCS = lambda do |pair|  #need filter out nil beforehand
  k1, counts = pair.first, pair.last
  total = counts.pop.last.to_f
  counts.map { |k2, count| [[k1, k2], count/total ] }
end

confidenceRDD = $sc.textFile( fileName )
                   .flatMap( singles_and_pairs )
                   .reduceByKey( lambda{ |a, b| a + b } )
                   .map( lambda{ |keys, count| [keys.first, [keys.last, count]] } )
                   .groupByKey()
                   .mapValues( sort_filter )
                   .filter(lambda{ |key, values| !values.nil? } )
                   .flatMap( calcCS )

puts "Top " + topN.to_s
# Currently there's no RDD.takeOrdered() in Ruby-spark. Consider using #max(topN) or #max_by(topN) instead
#print confidenceRDD.collect
#print confidenceRDD.sort_by(lambda{|itemSet, score| -score}).take(topN)
print confidenceRDD.reduce( lambda{|memo, item| memo[1] > item[1] ? memo : item } ).to_s # customization from #max for taking the item pair with the max score because Ruby-Spark don't have max_by yet.