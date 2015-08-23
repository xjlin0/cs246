# execfile('friend_you_may_know.py')

#fileName = 'q1testdata.txt'
import itertools

def parseFriendsLine(line):
  minimum = -9999999999
  user, friends = line.split('\t')
  friends = friends.split(',')
  connected = [((user, friend), minimum) for friend in friends]
  commons = [(pair, 1) for pair in list(itertools.permutations(friends, 2))]
  return connected + commons

fileName = 'soc-LiveJournal1Adj.txt'

friendsListRDD = (sc
                  .textFile( fileName, 16 )
                  .flatMap( parseFriendsLine )
                  .reduceByKey( lambda total, current: total + current )
                  .filter(lambda (pair, counts): counts > 0)
                  .map(lambda ((user, friend), counts): (user, (counts, friend)))
                  .groupByKey()
                  .map(lambda (user, suggestions):(user, sorted(list(suggestions), reverse=True)))
                  #.cache()
                   )


print "924"
print friendsListRDD.lookup('924')
#print friendsListRDD.collect()