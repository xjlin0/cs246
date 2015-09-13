# in jenv exec ruby-spark shell
# load('q1_people_you_may_know_spark.rb')

require 'ruby-spark'

parse_friends_line = lambda do |line|
  minimum, friends = -9999999999, nil
  user, friends = line.split("\t")
  if line.nil? || friends.nil?  #somebody got no friends...
    [ [user, 0] ]
  else
    friends = friends.split(",")
    connecteds = friends.map{ |friend| [[user, friend], minimum ] }
    commons = friends.permutation(2).map{ |pair| [pair, 1] }
    connecteds.concat( commons )
  end
end

file_name = 'soc-LiveJournal1Adj.txt'
#file_name = 'q1testdata.txt'
N = 10  #only ouput 10 most possible friends
friends_list_RDD = $sc
  .textFile( file_name )
  .flatMap( parse_friends_line )
  .reduceByKey( lambda{ |total, current| total + current } )
  .filter( lambda{ |pair, counts| counts > 0 } )
  .map( lambda{|pair, counts| [pair.first, [counts, pair.last]] } )
  .groupByKey
  .map( lambda{|user, suggestions| [user, suggestions.max( n ) ] } )
  .bind(n: N)
# .cache

#print friends_list_RDD.lookup('924')
#print friends_list_RDD.take(5)
#complain method missing...

#puts "924"
print friends_list_RDD.lookup('924')  #Need Ruby-Spark Aug 16 2015 version
# should be 439,2409,6995,11860,15416,43748,45881
