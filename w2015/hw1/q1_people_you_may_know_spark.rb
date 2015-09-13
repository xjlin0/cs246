# in jenv exec ruby-spark shell
# load('q1_people_you_may_know_spark.rb')

require 'ruby-spark'

parse_friends_line = lambda do |line|
  minimum, friends = -9999999999, nil
  user, friends = line.split("\t")
  if line.nil? || friends.nil?  #somebody got no friends...
    [ [user, nil] ]
  else
    friends = friends.split(",")
    connected = friends.map{ |friend| [[user, friend], minimum ] }
    commons = friends.permutation(2).map{ |pair| [pair, 1] }
    connected + commons
  end
end

file_name = 'soc-LiveJournal1Adj.txt'
#file_name = 'q1testdata.txt'

friends_list_RDD = $sc
  .textFile( file_name )
  .flatMap( parse_friends_line )
  .reduceByKey( lambda{ |total, current| total + current } )
  .filter( lambda{ |pair, counts| !counts.nil? && counts > 0 } )
  .map( lambda{|pair, counts| [pair.first, [counts, pair.last]] } )
  .groupByKey
  .map(lambda{|user, suggestions| [user, suggestions.sort.reverse ] }  )
# .cache

#print friends_list_RDD.lookup('924')
print friends_list_RDD.take(2)
#complain method missing...

#puts "924"
#print friendsListRDD.lookup('924')  #Need Ruby-Spark Aug 16 2015 version
# should be 439,2409,6995,11860,15416,43748,45881
