#load('./friend_you_may_know.rb')

require 'ruby-spark'

parse_friends_line = lambda do |line|
  minimum = -9999999999
  user, friends = line.split("\t")
  friends = friends.split(",")
  connected = friends.map{ |friend| [[user, friend], minimum ] }
  commons = friends.permutation(2).map{ |pair| [pair, 1] }
  connected + commons
end

#file_name = 'soc-LiveJournal1Adj.txt'
file_name = 'q1testdata.txt'
friends_list_RDD =  $sc
                  .textFile( file_name )
                  .flatMap( parse_friends_line )
                  .reduceByKey( lambda{ |total, current| total + current } )
                  .filter( lambda{ |pair, counts| counts > 0 } )
                  .map( lambda{|pair, counts| [pair.first, [counts, pair.last]] } )
                  .groupByKey
                  .map(lambda{|user, suggestions| [user, suggestions.sort.reverse ] }  )
                  .cache

print friends_list_RDD.collect
#puts friends_list_RDD.take(2)
#complain method missing...

#puts "924"
#print friendsListRDD.lookup('924')
# should be 439,2409,6995,11860,15416,43748,45881